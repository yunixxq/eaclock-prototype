/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;

// change
ChangeAlgorithm *Algorithm= NULL;
// change,lru
BufferNode *lruStack = NULL;
// change,EAclock
extern void UpdateWeight();
EAclockStrategyControl *EAclockControl = NULL;



/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
InitBufferPool(void)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundAlgorithm,
				foundlru,
				foundEAclock,
				foundBufCkpt;

// change,Algorithm 修改
	Algorithm = ShmemInitStruct("Algorithm Control",
								sizeof(ChangeAlgorithm),
								&foundAlgorithm);
	if (!foundAlgorithm)
	{
		Algorithm->algorithm_first = 's';
		Algorithm->algorithm_second = 's';

		Algorithm->GetFromFreelist = 0;
		Algorithm->GetFromCLOCK = 0;
		Algorithm->GetFromCLOCKSWEEP = 0;
		Algorithm->GetFromLRU = 0;
		Algorithm->GetFromRandom = 0;
		Algorithm->GetFromHyperbolic = 0;
		Algorithm->GetFromEAclock = 0;
		Algorithm->StrategyAccessBuffer = 0;
		Algorithm->EAclockFdwAgeTimes = 0;
	}
// change,lru,申请节点内存
	/* Align descriptors to a cacheline boundary. */
	lruStack = (BufferNode *)
		ShmemInitStruct("lruBufferNode",
						(NBuffers+10) * sizeof(BufferNode),
						&foundlru);
	if (!foundlru)
	{
// change,lru,初始化头尾标兵节点
		BufferNode *lru_head = &lruStack[NBuffers];
		BufferNode *lru_end = &lruStack[NBuffers+1];
		lru_head->next = lru_end;
		lru_end->prev = lru_head;
		lru_end->next = NULL;
		lru_head->prev = NULL;
		lru_head->node_id = NBuffers;
		lru_end->node_id = NBuffers+1;

		// Initialize LIRS and S3FIFO sentinel nodes
		for (int i = NBuffers + 2; i < NBuffers + 10; i++)
		{
			lruStack[i].node_id = i;
			lruStack[i].prev = NULL;
			lruStack[i].next = NULL;
			lruStack[i].isLIR = false;
			lruStack[i].isInbuf = false;
			lruStack[i].accessCount = 0;
		}
	}

// change,EAclock,申请全局控制器
	EAclockControl = (EAclockStrategyControl *)
		ShmemInitStruct("EAclockStrategyControl",
						sizeof(EAclockStrategyControl),
						&foundEAclock);
	if (!foundEAclock)
	{
		pg_atomic_init_u32(&EAclockControl->evictNum, 0);

		EAclockControl->HitInBuf = 0;
		EAclockControl->lastAction = 1;
		EAclockControl->lastHR = 0;
		EAclockControl->lastWeight = 2;
		EAclockControl->starCount = false;
		EAclockControl->isChange = false;
	}


	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStruct("Buffer Descriptors",
						NBuffers * sizeof(BufferDescPadded),
						&foundDescs);

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStruct("Buffer Blocks",
								  NBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &foundBufs));

	/* Align condition variables to cacheline boundary. */
	BufferIOCVArray = (ConditionVariableMinimallyPadded *)
		ShmemInitStruct("Buffer IO Condition Variables",
						NBuffers * sizeof(ConditionVariableMinimallyPadded),
						&foundIOCV);

	/*
	 * The array used to sort to-be-checkpointed buffer ids is located in
	 * shared memory, to avoid having to allocate significant amounts of
	 * memory at runtime. As that'd be in the middle of a checkpoint, or when
	 * the checkpointer is restarted, memory allocation failures would be
	 * painful.
	 */
	CkptBufferIds = (CkptSortItem *)
		ShmemInitStruct("Checkpoint BufferIds",
						NBuffers * sizeof(CkptSortItem), &foundBufCkpt);

	if (foundDescs || foundBufs || foundIOCV || foundBufCkpt)
	{
		/* should find all of these, or none of them */
		Assert(foundDescs && foundBufs && foundIOCV && foundBufCkpt);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		int			i;



		/*
		 * Initialize all the buffer headers.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			ClearBufferTag(&buf->tag);

			pg_atomic_init_u32(&buf->state, 0);
			buf->wait_backend_pgprocno = INVALID_PGPROCNO;

// change,clock,修改
			buf->flag=false;

// change,lru,初始化中间节点
			BufferNode *curr = &lruStack[i]; // focus on the node with node_id = buf_id
			curr->prev = NULL;
			curr->next = NULL;
			curr->node_id = i;
			curr->startTime = time(NULL);

// change,EAclock,初始化页面值
			pg_atomic_init_u32(&curr->Value, 0);

			buf->buf_id = i;

			/*
			 * Initially link all the buffers together as unused. Subsequent
			 * management of this list is done by freelist.c.
			 */
			buf->freeNext = i + 1;

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							 LWTRANCHE_BUFFER_CONTENT);

			ConditionVariableInit(BufferDescriptorGetIOCV(buf));
		}

		/* Correct last entry of linked list */
		GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
	}

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);
}

/*
 * BufferShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc.
 */
Size
BufferShmemSize(void)
{
	Size		size = 0;

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages, plus alignment padding */
	size = add_size(size, PG_IO_ALIGN_SIZE);
	size = add_size(size, mul_size(NBuffers, BLCKSZ));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	/* size of I/O condition variables */
	size = add_size(size, mul_size(NBuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of checkpoint sort array in bufmgr.c */
	size = add_size(size, mul_size(NBuffers, sizeof(CkptSortItem)));

	return size;
}
