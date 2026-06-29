/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include <math.h>
#include <string.h>


#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

// change,latency,延迟数据结构实现
EvictionLatencyData *LatencyData_EACLOCK = NULL;
EvictionLatencyData *LatencyData_LIRS = NULL;
EvictionLatencyData *LatencyData_S3FIFO = NULL;
EvictionLatencyData *LatencyData_WATT = NULL;
EvictionLatencyData *LatencyData_ARC = NULL;
EvictionLatencyData *LatencyData_Hyperbolic = NULL;
EvictionLatencyData *LatencyData_LRU2 = NULL;
EvictionLatencyData *LatencyData_CLOCK = NULL;

// change,LIRS,LIRS栈S和队列Q的哨兵节点
BufferNode *LIRS_S_head = NULL;
BufferNode *LIRS_S_tail = NULL;
BufferNode *LIRS_Q_head = NULL;
BufferNode *LIRS_Q_tail = NULL;
int LIRS_curr_LIR_sum = 0;
int LIRS_lirs_length = 0;
int LIRS_hirs_length = 0;

// change,S3FIFO,S3FIFO的三个队列哨兵节点
BufferNode *S3FIFO_S_head = NULL;
BufferNode *S3FIFO_S_tail = NULL;
BufferNode *S3FIFO_M_head = NULL;
BufferNode *S3FIFO_M_tail = NULL;
BufferNode *S3FIFO_G_head = NULL;
BufferNode *S3FIFO_G_tail = NULL;
int S3FIFO_s_length = 0;
int S3FIFO_m_length = 0;
int S3FIFO_g_length = 0;

// change,latency,初始化延迟数据结构(使用ShmemInitStruct分配到共享内存)
void InitEvictionLatency(bool init)
{
	bool found;

	/* Allocate all latency data in one shared memory segment */
	Size total_size = MAX_LATENCY_ALGOS * sizeof(EvictionLatencyData);
	char *base = (char *)ShmemInitStruct("Eviction Latency Data",
										 total_size, &found);
	if (!found)
		memset(base, 0, total_size);

	/* Point each algorithm's pointer to its slot */
	LatencyData_EACLOCK    = (EvictionLatencyData *)(base + 0 * sizeof(EvictionLatencyData));
	LatencyData_LIRS       = (EvictionLatencyData *)(base + 1 * sizeof(EvictionLatencyData));
	LatencyData_S3FIFO     = (EvictionLatencyData *)(base + 2 * sizeof(EvictionLatencyData));
	LatencyData_WATT       = (EvictionLatencyData *)(base + 3 * sizeof(EvictionLatencyData));
	LatencyData_ARC        = (EvictionLatencyData *)(base + 4 * sizeof(EvictionLatencyData));
	LatencyData_Hyperbolic = (EvictionLatencyData *)(base + 5 * sizeof(EvictionLatencyData));
	LatencyData_LRU2       = (EvictionLatencyData *)(base + 6 * sizeof(EvictionLatencyData));
	LatencyData_CLOCK      = (EvictionLatencyData *)(base + 7 * sizeof(EvictionLatencyData));

	if (!found)
	{
		SpinLockInit(&LatencyData_EACLOCK->lat_lock);
		SpinLockInit(&LatencyData_LIRS->lat_lock);
		SpinLockInit(&LatencyData_S3FIFO->lat_lock);
		SpinLockInit(&LatencyData_WATT->lat_lock);
		SpinLockInit(&LatencyData_ARC->lat_lock);
		SpinLockInit(&LatencyData_Hyperbolic->lat_lock);
		SpinLockInit(&LatencyData_LRU2->lat_lock);
		SpinLockInit(&LatencyData_CLOCK->lat_lock);

		pg_atomic_init_u32(&LatencyData_EACLOCK->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_LIRS->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_S3FIFO->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_WATT->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_ARC->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_Hyperbolic->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_LRU2->sample_count, 0);
		pg_atomic_init_u32(&LatencyData_CLOCK->sample_count, 0);
	}
}

// change,latency,记录一次驱逐延迟
static pg_atomic_uint64 total_evictions;
void RecordEvictionLatency(EvictionLatencyData *data, uint64 ns)
{
	if (data == NULL) return;
	uint32 idx = pg_atomic_fetch_add_u32(&data->sample_count, 1);
	if (idx < MAX_LIRS_SAMPLES)
		data->latencies[idx] = ns;
}

uint64 GetTotalEvictions(void) { return pg_atomic_read_u64(&total_evictions); }

// change,latency,计算并输出百分位数(使用简单排序,针对小样本)
static uint64 compute_percentile(uint64 *arr, uint32 n, double pct)
{
	if (n == 0) return 0;
	// Use static buffer to avoid backend malloc/free — safe since compute_percentile
	// is called single-threaded from PrintEvictionLatency during command execution.
	static uint64 sort_tmp[MAX_LIRS_SAMPLES];
	uint32 copy_n = (n < MAX_LIRS_SAMPLES) ? n : MAX_LIRS_SAMPLES;
	memcpy(sort_tmp, arr, sizeof(uint64) * copy_n);
	for (uint32 i = 1; i < copy_n; i++)
	{
		uint64 key = sort_tmp[i];
		uint32 j = i;
		while (j > 0 && sort_tmp[j-1] > key) { sort_tmp[j] = sort_tmp[j-1]; j--; }
		sort_tmp[j] = key;
	}
	return sort_tmp[(uint32)(copy_n * pct / 100.0)];
}

void PrintEvictionLatency(const char *algo_name, EvictionLatencyData *data)
{
	if (data == NULL) { elog(LOG, "%s: no data", algo_name); return; }
	uint32 n = pg_atomic_read_u32(&data->sample_count);
	uint32 buf_sz = pg_atomic_read_u32(&data->buf_size);
	if (n == 0) { elog(LOG, "%s: 0 samples", algo_name); return; }

	uint64 p50 = compute_percentile(data->latencies, n, 50);
	uint64 p90 = compute_percentile(data->latencies, n, 90);
	uint64 p99 = compute_percentile(data->latencies, n, 99);
	uint64 max = data->latencies[0];
	for (uint32 i = 1; i < n && i < MAX_LIRS_SAMPLES; i++)
		if (data->latencies[i] > max) max = data->latencies[i];

	elog(LOG, "%s: buf_size=%u samples=%u P50=%llu ns P90=%llu ns P99=%llu ns Max=%llu ns",
		 algo_name, buf_sz, n,
		 (unsigned long long)p50, (unsigned long long)p90,
		 (unsigned long long)p99, (unsigned long long)max);
}

// change,latency,打印所有算法延迟
void PrintAllEvictionLatency(void)
{
	elog(LOG, "=== Eviction Latency Results ===");
	PrintEvictionLatency("EACLOCK",   LatencyData_EACLOCK);
	PrintEvictionLatency("LIRS",      LatencyData_LIRS);
	PrintEvictionLatency("S3FIFO",   LatencyData_S3FIFO);
	PrintEvictionLatency("WATT",      LatencyData_WATT);
	PrintEvictionLatency("ARC",       LatencyData_ARC);
	PrintEvictionLatency("Hyperbolic",LatencyData_Hyperbolic);
	PrintEvictionLatency("LRU-2",    LatencyData_LRU2);
	PrintEvictionLatency("CLOCK",     LatencyData_CLOCK);

	// Also write to a persistent file so data survives server restarts
	FILE *f = fopen("pg_eviction_latency.txt", "w");
	if (f)
	{
		fprintf(f, "=== Eviction Latency Results ===\n");
	#define WRITE_ALGO(name, data) do { \
			if (data) { \
				uint32 n = pg_atomic_read_u32(&data->sample_count); \
				uint32 bs = pg_atomic_read_u32(&data->buf_size); \
				if (n > 0) { \
					uint64 p50 = compute_percentile(data->latencies, n, 50); \
					uint64 p90 = compute_percentile(data->latencies, n, 90); \
					uint64 p99 = compute_percentile(data->latencies, n, 99); \
					fprintf(f, "%s: buf_size=%u samples=%u P50=%llu ns P90=%llu ns P99=%llu ns\n", \
						name, bs, n, (unsigned long long)p50, \
						(unsigned long long)p90, (unsigned long long)p99); \
				} else { \
					fprintf(f, "%s: 0 samples\n", name); \
				} \
			} else { \
				fprintf(f, "%s: no data\n", name); \
			} \
		} while(0)
		WRITE_ALGO("EACLOCK",    LatencyData_EACLOCK);
		WRITE_ALGO("LIRS",       LatencyData_LIRS);
		WRITE_ALGO("S3FIFO",    LatencyData_S3FIFO);
		WRITE_ALGO("WATT",      LatencyData_WATT);
		WRITE_ALGO("ARC",       LatencyData_ARC);
		WRITE_ALGO("Hyperbolic",LatencyData_Hyperbolic);
		WRITE_ALGO("LRU-2",    LatencyData_LRU2);
		WRITE_ALGO("CLOCK",     LatencyData_CLOCK);
#undef WRITE_ALGO
		fclose(f);
	}
}


// change,lru
void StrategyAccessBuffer(int buf_id, bool delete); 
// change,EAclock
extern void UpdateWeight();

/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * Clock sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 */
	pg_atomic_uint32 nextVictimBuffer;

	int			firstFreeBuffer;	/* Head of list of unused buffers */
	int			lastFreeBuffer; /* Tail of list of unused buffers */

// change,lru
	slock_t 	lru_lock;
	// Pointer to the top and bottom nodes of the BufferNode stack.
	BufferNode *stackTop;
	BufferNode *stackBottom;

	/*
	 * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
	 * when the list is empty)
	 */

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 */
	int			bgwprocno;
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint32 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32		victim;

	/*
	 * Atomically move hand ahead one buffer - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	victim =
		pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer, 1);

	if (victim >= NBuffers)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in BufferDescriptors */
		victim = victim % NBuffers;

		/*
		 * If we're the one that just caused a wraparound, force
		 * completePasses to be incremented while holding the spinlock. We
		 * need the spinlock so StrategySyncStart() can return a consistent
		 * value consisting of nextVictimBuffer and completePasses.
		 */
		if (victim == 0)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				/*
				 * Acquire the spinlock while increasing completePasses. That
				 * allows other readers to read nextVictimBuffer and
				 * completePasses in a consistent manner which is required for
				 * StrategySyncStart().  In theory delaying the increment
				 * could lead to an overflow of nextVictimBuffers, but that's
				 * highly unlikely and wouldn't be particularly harmful.
				 */
				SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

				wrapped = expected % NBuffers;

				success = pg_atomic_compare_exchange_u32(&StrategyControl->nextVictimBuffer,
														 &expected, wrapped);
				if (success)
					StrategyControl->completePasses++;
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
			}
		}
	}
	return victim;
}



/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 */
bool
have_free_buffer(void)
{
	if (StrategyControl->firstFreeBuffer >= 0)
		return true;
	else
		return false;
}


// Adjusts the position of buffer (identified by buf_id) in the LRU stack if delete is false;
// otherwise, delete buffer buf_id from the LRU stack.

void
StrategyAccessBuffer(int buf_id, bool delete)
{

	Algorithm->StrategyAccessBuffer++;

	if (buf_id < 0 || buf_id >= NBuffers)
	{
		elog(ERROR, "Invalid buffer index");
	}

	// WATT: 纯原子计数，无需锁
	if (Algorithm->algorithm_first == 'w')
	{
		BufferNode *curr_w = &lruStack[buf_id];
		pg_atomic_fetch_add_u32(&curr_w->Value, 1);
	}

	// LIRS: 持锁保护 S 栈和 Q 队列
	if (Algorithm->algorithm_first == 'i')
	{
		SpinLockAcquire(&StrategyControl->lru_lock);

		BufferNode *curr = &lruStack[buf_id];
		curr->isInbuf = true;

		if (!curr->isLIR)
		{
			// HIR命中（Q中）：晋升为LIR，插入S栈顶
			curr->isLIR = true;
			LIRS_curr_LIR_sum++;

			// 从Q中摘除
			BufferNode *q_curr = LIRS_Q_head->next;
			while (q_curr != LIRS_Q_tail)
			{
				if (q_curr->node_id == buf_id)
				{
					q_curr->prev->next = q_curr->next;
					q_curr->next->prev = q_curr->prev;
					break;
				}
				q_curr = q_curr->next;
			}

			// 插入S栈顶
			curr->prev = LIRS_S_head;
			curr->next = LIRS_S_head->next;
			LIRS_S_head->next->prev = curr;
			LIRS_S_head->next = curr;

			if (LIRS_curr_LIR_sum > LIRS_lirs_length)
			{
				BufferNode *tail = LIRS_S_tail->prev;
				if (tail != LIRS_S_head && tail->isLIR)
				{
					tail->isLIR = false;
					LIRS_curr_LIR_sum--;

					tail->prev->next = tail->next;
					tail->next->prev = tail->prev;
					tail->prev = LIRS_Q_head;
					tail->next = LIRS_Q_head->next;
					LIRS_Q_head->next->prev = tail;
					LIRS_Q_head->next = tail;
				}
			}
		}
		else
		{
			// LIR命中：若不在栈顶则移动到S栈顶
			if (curr->prev != LIRS_S_head)
			{
				curr->prev->next = curr->next;
				curr->next->prev = curr->prev;
				curr->prev = LIRS_S_head;
				curr->next = LIRS_S_head->next;
				LIRS_S_head->next->prev = curr;
				LIRS_S_head->next = curr;
			}
		}
		SpinLockRelease(&StrategyControl->lru_lock);
	}

	// S3FIFO: 持锁保护 S/M/G 三个队列
	if (Algorithm->algorithm_first == '3')
	{
		SpinLockAcquire(&StrategyControl->lru_lock);

		BufferNode *curr = &lruStack[buf_id];
		curr->accessCount++;

		// 命中3次以上：从S晋升到M
		if (curr->accessCount >= 3)
		{
			if (curr->prev != NULL &&
				curr != S3FIFO_S_head && curr != S3FIFO_S_tail)
			{
				curr->prev->next = curr->next;
				curr->next->prev = curr->prev;
				curr->prev = S3FIFO_M_head;
				curr->next = S3FIFO_M_head->next;
				S3FIFO_M_head->next->prev = curr;
				S3FIFO_M_head->next = curr;
				curr->accessCount = 0;
			}
		}

		// G幽灵命中：从G摘除，插入S栈顶
		BufferNode *g = S3FIFO_G_head->next;
		while (g != S3FIFO_G_tail)
		{
			if (g->node_id == buf_id)
			{
				g->prev->next = g->next;
				g->next->prev = g->prev;
				g->prev = S3FIFO_S_head;
				g->next = S3FIFO_S_head->next;
				S3FIFO_S_head->next->prev = g;
				S3FIFO_S_head->next = g;
				g->accessCount = 1;
				break;
			}
			g = g->next;
		}

		SpinLockRelease(&StrategyControl->lru_lock);
	}

}

// hyperbolic sample 
int hyperbolic_sample()
{
	BufferDesc *buf;
	uint32		local_buf_state;

	time_t cur_time = time(NULL);
	int min = 0;
	int max = NBuffers;
	double min_hyperbolic = 10000;
	int random_number;
	int tmp;
	double count;
	int buf_id;
	BufferNode *curr;
	double timeDifference;
	int min_hyperbolic_buf_id = 0;

	for(int i=0;i<20;i++)
	{
		random_number = drand48() * (max - min + 1) + min;
		buf_id= random_number%NBuffers;
		curr = &lruStack[buf_id]; 
		timeDifference = difftime(cur_time, curr->startTime);
		buf = GetBufferDescriptor(buf_id);

		count = BUF_STATE_GET_USAGECOUNT(pg_atomic_read_u32(&buf->state));

		double frequency = count / timeDifference;
		if(frequency<min_hyperbolic)
		{
			min_hyperbolic_buf_id = buf_id;
			min_hyperbolic = frequency;
		}
	}
	return min_hyperbolic_buf_id;
}



/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */

	*from_ring = false;

	strategy = NULL;


	bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		StrategyControl->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 */
	if (StrategyControl->firstFreeBuffer >= 0)
	{
		while (true)
		{
			/* Acquire the spinlock to remove element from the freelist */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

			if (StrategyControl->firstFreeBuffer < 0)
			{
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
				break;
			}

			buf = GetBufferDescriptor(StrategyControl->firstFreeBuffer);
			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Unconditionally remove buffer from freelist */
			StrategyControl->firstFreeBuffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			/*
			 * Release the lock so someone else can access the freelist while
			 * we check out this buffer.
			 */
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; discard it and retry.  (This can only happen if VACUUM
			 * put a valid buffer in the freelist and then someone else used
			 * it before we got to it.  It's probably impossible altogether as
			 * of 8.3, but we'd better check anyway.)
			 */
			local_buf_state = LockBufHdr(buf);
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
				&& BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
			{
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
// change,hyperbolic
				if(Algorithm->algorithm_first == 'h')
				{
					BufferNode *curr = &lruStack[buf->buf_id];
					curr->startTime = time(NULL);
				}
// change,lru
				StrategyAccessBuffer(buf->buf_id, false);

				Algorithm->GetFromFreelist++;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}

// change


// change,EAclock,
	if(Algorithm->algorithm_first == 'e')
	{
		Algorithm->GetFromEAclock++;
		/* Nothing on the freelist, so run the "EAclock" algorithm */
		EAclockControl->starCount = true;
		trycounter = NBuffers;
		instr_time t0, t1;
		INSTR_TIME_SET_CURRENT(t0);
		for (;;)
		{
			buf = GetBufferDescriptor(ClockSweepTick());

			BufferNode *buf_lru_node = &lruStack[buf->buf_id];

			if (BUF_STATE_GET_REFCOUNT((pg_atomic_read_u32(&buf->state))) == 0)
			{
				if (pg_atomic_read_u32(&buf_lru_node->Value) >= 1)
				{
					if(EAclockControl->isChange == true)
					{
						int value = pg_atomic_read_u32(&buf_lru_node->Value);
						pg_atomic_fetch_sub_u32(&buf_lru_node->Value,value/2);
					}
					else
						pg_atomic_fetch_sub_u32(&buf_lru_node->Value,1);
					trycounter = NBuffers;
				}
				else
				{
					local_buf_state = LockBufHdr(buf);
					*buf_state = local_buf_state;
					ValueInit(buf->buf_id);
					if(pg_atomic_fetch_add_u32(&EAclockControl->evictNum,1)>=NBuffers/2)
					{
						UpdateWeight();
						pg_atomic_init_u32(&EAclockControl->evictNum, 0);
						EAclockControl->HitInBuf = 0;
					}
					INSTR_TIME_SET_CURRENT(t1);
					RecordEvictionLatency(LatencyData_EACLOCK,
						(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
					return buf;
				}
			}
			else if (--trycounter == 0)
			{
				elog(ERROR, "no unpinned buffers available");
			}
		}
	}
// change,lru
	else if(Algorithm->algorithm_first == 'l')
	{

		Algorithm->GetFromLRU++;
		instr_time t0, t1;
		INSTR_TIME_SET_CURRENT(t0);
again:
		SpinLockAcquire(&StrategyControl->lru_lock);
		// Get victim buffer from the tail of list, which means the bottom of the stack.
		BufferNode *victim = StrategyControl->stackBottom->prev;
		int buf_id = victim->node_id;
	
		while( buf_id>=0 && buf_id<NBuffers)
		{

			buf = GetBufferDescriptor(buf_id);

			// For lru implementation, usage_count is not important or ignored.
			
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				*buf_state = local_buf_state;
				SpinLockRelease(&StrategyControl->lru_lock);
				INSTR_TIME_SET_CURRENT(t1);
				RecordEvictionLatency(LatencyData_LRU2,
					(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
				return buf;
			}
			else if (StrategyControl->stackTop->next->node_id == buf->buf_id)
			{
				UnlockBufHdr(buf, local_buf_state);
				SpinLockRelease(&StrategyControl->lru_lock);
				goto again;
			}
			
			UnlockBufHdr(buf, local_buf_state);


			victim = victim->prev;
			buf_id = victim->node_id;
        }

        SpinLockRelease(&StrategyControl->lru_lock);

	}
// clock sweep
	else if(Algorithm->algorithm_first == 's')
	{
		
		Algorithm->GetFromCLOCKSWEEP++;
		/* Nothing on the freelist, so run the "clock sweep" algorithm */
		trycounter = NBuffers;
		instr_time t0, t1;
		INSTR_TIME_SET_CURRENT(t0);
		for (;;)
		{
			buf = GetBufferDescriptor(ClockSweepTick());

			/*
			* If the buffer is pinned or has a nonzero usage_count, we cannot use
			* it; decrement the usage_count (unless pinned) and keep scanning.
			*/
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
				{
					local_buf_state -= BUF_USAGECOUNT_ONE;
					trycounter = NBuffers;
				}
				else
				{
					/* Found a usable buffer */
					if (strategy != NULL)
						AddBufferToRing(strategy, buf);
					*buf_state = local_buf_state;
					INSTR_TIME_SET_CURRENT(t1);
					RecordEvictionLatency(LatencyData_LRU2,
						(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
					return buf;
				}
			}
			else if (--trycounter == 0)
			{
				/*
				* We've scanned all the buffers without making any state changes,
				* so all the buffers are pinned (or were when we looked at them).
				* We could hope that someone will free one eventually, but it's
				* probably better to fail than to risk getting stuck in an
				* infinite loop.
				*/
				UnlockBufHdr(buf, local_buf_state);
				elog(ERROR, "no unpinned buffers available");
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}
// change,clock
	else if(Algorithm->algorithm_first == 'c')
	{
		Algorithm->GetFromCLOCK++;
		trycounter = NBuffers;
		instr_time t0, t1;
		INSTR_TIME_SET_CURRENT(t0);
		for (;;)
		{
			buf = GetBufferDescriptor(ClockSweepTick());

			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				if(buf->flag==false)
				{
					/* Found a usable buffer */
					if (strategy != NULL)
						AddBufferToRing(strategy, buf);
					*buf_state = local_buf_state;
					buf->flag = true;
					INSTR_TIME_SET_CURRENT(t1);
					RecordEvictionLatency(LatencyData_CLOCK,
						(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
					return buf;
				}
				else
				{
					buf->flag=false;
				}
			}
			else if (--trycounter == 0)
			{
				UnlockBufHdr(buf, local_buf_state);
				elog(ERROR, "no unpinned buffers available");
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}
// change,random
	else if(Algorithm->algorithm_first == 'r')
	{
		Algorithm->GetFromRandom++;

		int min = 0;
		int max = NBuffers;
		for (;;)
		{
			int random_number = drand48() * (max - min + 1) + min;
			random_number= random_number%NBuffers;
			buf = GetBufferDescriptor(random_number);

			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				/* Found a usable buffer */
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);	
		}		
	}
// change,Hyperbolic
	else if(Algorithm->algorithm_first == 'h')
	{
		Algorithm->GetFromHyperbolic++;
		instr_time t0, t1;

		INSTR_TIME_SET_CURRENT(t0);
		for (;;)
		{
			buf = GetBufferDescriptor(hyperbolic_sample());

			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				*buf_state = local_buf_state;
				BufferNode *curr = &lruStack[buf->buf_id];
				curr->startTime = time(NULL);
				INSTR_TIME_SET_CURRENT(t1);
				RecordEvictionLatency(LatencyData_Hyperbolic,
					(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}
// change,LIRS,'i' for LIRS eviction
	else if(Algorithm->algorithm_first == 'i')
	{
		Algorithm->GetFromEAclock++; // reuse counter
		instr_time t0, t1;

		INSTR_TIME_SET_CURRENT(t0);

		// LIRS eviction: scan S from head, if LIR found return it; else scan Q tail
		BufferNode *victim_node = NULL;
		BufferNode *curr_s = LIRS_S_head->next;

		while (curr_s != LIRS_S_tail)
		{
			int buf_id = curr_s->node_id;
			buf = GetBufferDescriptor(buf_id);
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				if (curr_s->isLIR)
				{
					// Found LIR in S - evict it, insert new data as HIR in Q
					BufferNode *s_node = curr_s;
					s_node->prev->next = s_node->next;
					s_node->next->prev = s_node->prev;
					s_node->isInbuf = false;
					s_node->isLIR = false;
					s_node->prev = NULL;
					s_node->next = NULL;
					LIRS_curr_LIR_sum--;

					*buf_state = local_buf_state;
					INSTR_TIME_SET_CURRENT(t1);
					RecordEvictionLatency(LatencyData_LIRS,
						(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));

					// 新数据进入：插入Q作为HIR
					BufferNode *new_node = &lruStack[buf_id];
					new_node->isInbuf = true;
					new_node->isLIR = false;
					new_node->prev = LIRS_Q_head;
					new_node->next = LIRS_Q_head->next;
					LIRS_Q_head->next->prev = new_node;
					LIRS_Q_head->next = new_node;

					return buf;
				}
				UnlockBufHdr(buf, local_buf_state);
			}
			else
			{
				UnlockBufHdr(buf, local_buf_state);
			}
			curr_s = curr_s->next;
		}

		// No LIR found in S, evict from Q tail (HIR resident)
		victim_node = LIRS_Q_tail->prev;
		if (victim_node != LIRS_Q_head)
		{
			int victim_buf_id = victim_node->node_id;
			victim_node->prev->next = victim_node->next;
			victim_node->next->prev = victim_node->prev;
			victim_node->isInbuf = false;
			// Reset LIR flag since this buffer now holds new data
			victim_node->isLIR = false;

			buf = GetBufferDescriptor(victim_buf_id);
			local_buf_state = LockBufHdr(buf);
			*buf_state = local_buf_state;
			INSTR_TIME_SET_CURRENT(t1);
			RecordEvictionLatency(LatencyData_LIRS,
				(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));

			// 将新进入的buffer插入Q作为HIR（新数据，stack distance=∞）
			BufferNode *new_node = &lruStack[victim_buf_id];
			new_node->isInbuf = true;
			new_node->isLIR = false;
			new_node->prev = LIRS_Q_head;
			new_node->next = LIRS_Q_head->next;
			LIRS_Q_head->next->prev = new_node;
			LIRS_Q_head->next = new_node;

			// 栈剪枝：S栈底部若存在非LIR节点，将其降级到Q
			BufferNode *tail = LIRS_S_tail->prev;
			while (tail != LIRS_S_head && !tail->isLIR)
			{
				tail->isLIR = false;
				LIRS_curr_LIR_sum--;
				tail->prev->next = tail->next;
				tail->next->prev = tail->prev;
				tail->prev = LIRS_Q_head;
				tail->next = LIRS_Q_head->next;
				LIRS_Q_head->next->prev = tail;
				LIRS_Q_head->next = tail;
				tail = LIRS_S_tail->prev;
			}

			return buf;
		}

		// Fallback: scan all buffers
		elog(WARNING, "LIRS: no victim found, scanning all buffers");
		INSTR_TIME_SET_CURRENT(t1);
		RecordEvictionLatency(LatencyData_LIRS,
			(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
		buf = GetBufferDescriptor(ClockSweepTick());
		local_buf_state = LockBufHdr(buf);
		*buf_state = local_buf_state;
		return buf;
	}
// change,S3FIFO,'3' for S3FIFO eviction
	else if(Algorithm->algorithm_first == '3')
	{
		Algorithm->GetFromCLOCK++; // reuse counter
		instr_time t0, t1;

		INSTR_TIME_SET_CURRENT(t0);

		// S3FIFO eviction: first try S queue, then M queue
		BufferNode *victim_node = NULL;
		BufferNode *curr = S3FIFO_S_tail->prev; // scan from tail (FIFO)

		while (curr != S3FIFO_S_head)
		{
			int buf_id = curr->node_id;
			buf = GetBufferDescriptor(buf_id);
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				// 摘除前保存前驱指针（用于降级后继续扫描）
				BufferNode *prev_s = curr->prev;
				curr->prev->next = curr->next;
				curr->next->prev = curr->prev;

				if (curr->accessCount > 0)
				{
					// accessCount>0：降级放入M头部，继续扫描S
					curr->prev = S3FIFO_M_head;
					curr->next = S3FIFO_M_head->next;
					S3FIFO_M_head->next->prev = curr;
					S3FIFO_M_head->next = curr;
					curr->accessCount = 0;
					UnlockBufHdr(buf, local_buf_state);
					// 若 S 已空则退出 S 扫描；否则继续从 prev_s 扫描
					if (prev_s == S3FIFO_S_head)
						break;
					curr = prev_s;
					continue;
				}
				else
				{
					// 真正淘汰：将新数据插入S队列头部
					*buf_state = local_buf_state;
					BufferNode *new_node = &lruStack[buf_id];
					new_node->accessCount = 0;
					new_node->prev = S3FIFO_S_head;
					new_node->next = S3FIFO_S_head->next;
					S3FIFO_S_head->next->prev = new_node;
					S3FIFO_S_head->next = new_node;

					// victim插入G幽灵队列头部
					curr->prev = S3FIFO_G_head;
					curr->next = S3FIFO_G_head->next;
					S3FIFO_G_head->next->prev = curr;
					S3FIFO_G_head->next = curr;
					INSTR_TIME_SET_CURRENT(t1);
					RecordEvictionLatency(LatencyData_S3FIFO,
						(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
					return buf;
				}
			}
			else
			{
				UnlockBufHdr(buf, local_buf_state);
				curr = curr->prev;
			}
		}

		// S is empty or all pinned, scan M
		curr = S3FIFO_M_tail->prev;
		while (curr != S3FIFO_M_head)
		{
			int buf_id = curr->node_id;
			buf = GetBufferDescriptor(buf_id);
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				*buf_state = local_buf_state;
				curr->prev->next = curr->next;
				curr->next->prev = curr->prev;

				// 将新数据插入S队列头部
				BufferNode *new_node = &lruStack[buf_id];
				new_node->accessCount = 0;
				new_node->prev = S3FIFO_S_head;
				new_node->next = S3FIFO_S_head->next;
				S3FIFO_S_head->next->prev = new_node;
				S3FIFO_S_head->next = new_node;

				// victim插入G幽灵队列头部
				curr->prev = S3FIFO_G_head;
				curr->next = S3FIFO_G_head->next;
				S3FIFO_G_head->next->prev = curr;
				S3FIFO_G_head->next = curr;
				INSTR_TIME_SET_CURRENT(t1);
				RecordEvictionLatency(LatencyData_S3FIFO,
					(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
			curr = curr->prev;
		}

		// Fallback
		elog(WARNING, "S3FIFO: no victim found");
		INSTR_TIME_SET_CURRENT(t1);
		RecordEvictionLatency(LatencyData_S3FIFO,
			(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
		buf = GetBufferDescriptor(ClockSweepTick());
		local_buf_state = LockBufHdr(buf);
		*buf_state = local_buf_state;
		return buf;
	}
// change,WATT,'w' for WATT (LFU-based eviction with sampling)
	else if(Algorithm->algorithm_first == 'w')
	{
		Algorithm->GetFromEAclock++;
		instr_time t0, t1;
		INSTR_TIME_SET_CURRENT(t0);

		int sample_count = 8;
		BufferDesc *candidates[8];
		int best_buf_id = -1;
		double best_freq = -1.0;
		time_t now = time(NULL);

		for (int i = 0; i < sample_count; i++)
		{
			int idx = (int)(drand48() * NBuffers);
			candidates[i] = GetBufferDescriptor(idx);
		}

		for (int i = 0; i < sample_count; i++)
		{
			BufferDesc *cand = candidates[i];
			uint32 c_state = LockBufHdr(cand);
			if (BUF_STATE_GET_REFCOUNT(c_state) != 0)
			{
				UnlockBufHdr(cand, c_state);
				continue;
			}
			UnlockBufHdr(cand, c_state);

			BufferNode *node = &lruStack[cand->buf_id];
			double count = pg_atomic_read_u32(&node->Value);
			double age = difftime(now, node->startTime);
			if (age < 0.001) age = 0.001;
			double freq = count / age;
			if (freq > best_freq)
			{
				best_freq = freq;
				best_buf_id = cand->buf_id;
			}
		}

		if (best_buf_id >= 0)
		{
			buf = GetBufferDescriptor(best_buf_id);
			local_buf_state = LockBufHdr(buf);
			*buf_state = local_buf_state;
			INSTR_TIME_SET_CURRENT(t1);
			RecordEvictionLatency(LatencyData_WATT,
				(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
			return buf;
		}

		INSTR_TIME_SET_CURRENT(t1);
		RecordEvictionLatency(LatencyData_WATT,
			(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
		buf = GetBufferDescriptor(ClockSweepTick());
		local_buf_state = LockBufHdr(buf);
		*buf_state = local_buf_state;
		return buf;
	}
// change,ARC,'a' for ARC
	else if(Algorithm->algorithm_first == 'a')
	{
		Algorithm->GetFromCLOCK++;
		instr_time t0, t1;
		INSTR_TIME_SET_CURRENT(t0);

		// ARC: replace from LRU list (reuse the standard LRU list)
		SpinLockAcquire(&StrategyControl->lru_lock);
		BufferNode *victim = StrategyControl->stackBottom->prev;
		int buf_id = victim->node_id;

		while (buf_id >= 0 && buf_id < NBuffers)
		{
			buf = GetBufferDescriptor(buf_id);
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			{
				*buf_state = local_buf_state;
				SpinLockRelease(&StrategyControl->lru_lock);
				INSTR_TIME_SET_CURRENT(t1);
				RecordEvictionLatency(LatencyData_ARC,
					(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
				return buf;
			}
			else if (StrategyControl->stackTop->next->node_id == buf->buf_id)
			{
				// Top of LRU is pinned, restart
				UnlockBufHdr(buf, local_buf_state);
				SpinLockRelease(&StrategyControl->lru_lock);
				victim = StrategyControl->stackBottom->prev;
				buf_id = victim->node_id;
				continue;
			}

			UnlockBufHdr(buf, local_buf_state);
			victim = victim->prev;
			buf_id = victim->node_id;
		}
		SpinLockRelease(&StrategyControl->lru_lock);
		INSTR_TIME_SET_CURRENT(t1);
		RecordEvictionLatency(LatencyData_ARC,
			(uint64)(INSTR_TIME_GET_NANOSEC(t1) - INSTR_TIME_GET_NANOSEC(t0)));
		buf = GetBufferDescriptor(ClockSweepTick());
		local_buf_state = LockBufHdr(buf);
		*buf_state = local_buf_state;
		return buf;
	}

}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 */
void
StrategyFreeBuffer(BufferDesc *buf)
{
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		buf->freeNext = StrategyControl->firstFreeBuffer;
		if (buf->freeNext < 0)
			StrategyControl->lastFreeBuffer = buf->buf_id;
		StrategyControl->firstFreeBuffer = buf->buf_id;
	}

	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}

/*
 * StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	uint32		nextVictimBuffer;
	int			result;

	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	nextVictimBuffer = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
	result = nextVictimBuffer % NBuffers;

	if (complete_passes)
	{
		*complete_passes = StrategyControl->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += nextVictimBuffer / NBuffers;
	}

	if (num_buf_alloc)
	{
		*num_buf_alloc = pg_atomic_exchange_u32(&StrategyControl->numBufferAllocs, 0);
	}
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	StrategyControl->bgwprocno = bgwprocno;
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}


/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	/* size of eviction latency data for all algorithms */
	size = add_size(size, MAXALIGN(MAX_LATENCY_ALGOS * sizeof(EvictionLatencyData)));

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
StrategyInitialize(bool init)
{
	bool		found;

	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						sizeof(BufferStrategyControl),
						&found);

	if (!found)
	{
		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		SpinLockInit(&StrategyControl->buffer_strategy_lock);

		/*
		 * Grab the whole linked list of free buffers for our strategy. We
		 * assume it was previously set up by InitBufferPool().
		 */
		StrategyControl->firstFreeBuffer = 0;
		StrategyControl->lastFreeBuffer = NBuffers - 1;

		/* Initialize the clock sweep pointer */
		pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);

		/* Clear statistics */
		StrategyControl->completePasses = 0;
		pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);

		/* No pending notification */
		StrategyControl->bgwprocno = -1;

		// change,latency,初始化延迟测量数据
		InitEvictionLatency(true);
		pg_atomic_init_u32(&LatencyData_EACLOCK->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_LIRS->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_S3FIFO->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_WATT->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_ARC->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_Hyperbolic->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_LRU2->buf_size, NBuffers);
		pg_atomic_init_u32(&LatencyData_CLOCK->buf_size, NBuffers);

		// change,LIRS,初始化LIRS栈S和队列Q的哨兵节点
		SpinLockInit(&StrategyControl->lru_lock);
		SpinLockAcquire(&StrategyControl->lru_lock);

		LIRS_S_head = &lruStack[NBuffers];
		LIRS_S_tail = &lruStack[NBuffers + 1];
		LIRS_Q_head = &lruStack[NBuffers + 2];
		LIRS_Q_tail = &lruStack[NBuffers + 3];

		LIRS_S_head->prev = NULL;
		LIRS_S_head->next = LIRS_S_tail;
		LIRS_S_tail->prev = LIRS_S_head;
		LIRS_S_tail->next = NULL;

		LIRS_Q_head->prev = NULL;
		LIRS_Q_head->next = LIRS_Q_tail;
		LIRS_Q_tail->prev = LIRS_Q_head;
		LIRS_Q_tail->next = NULL;

		LIRS_lirs_length = (int)(0.95 * NBuffers);
		LIRS_hirs_length = NBuffers - LIRS_lirs_length;
		LIRS_curr_LIR_sum = 0;

		// change,S3FIFO,初始化S3FIFO三个队列的哨兵节点
		S3FIFO_S_head = &lruStack[NBuffers + 4];
		S3FIFO_S_tail = &lruStack[NBuffers + 5];
		S3FIFO_M_head = &lruStack[NBuffers + 6];
		S3FIFO_M_tail = &lruStack[NBuffers + 7];
		S3FIFO_G_head = &lruStack[NBuffers + 8];
		S3FIFO_G_tail = &lruStack[NBuffers + 9];

		S3FIFO_S_head->prev = NULL; S3FIFO_S_head->next = S3FIFO_S_tail;
		S3FIFO_S_tail->prev = S3FIFO_S_head; S3FIFO_S_tail->next = NULL;

		S3FIFO_M_head->prev = NULL; S3FIFO_M_head->next = S3FIFO_M_tail;
		S3FIFO_M_tail->prev = S3FIFO_M_head; S3FIFO_M_tail->next = NULL;

		S3FIFO_G_head->prev = NULL; S3FIFO_G_head->next = S3FIFO_G_tail;
		S3FIFO_G_tail->prev = S3FIFO_G_head; S3FIFO_G_tail->next = NULL;

		S3FIFO_s_length = (int)(0.1 * NBuffers);
		S3FIFO_m_length = NBuffers - S3FIFO_s_length;
		S3FIFO_g_length = NBuffers - S3FIFO_s_length;

		SpinLockRelease(&StrategyControl->lru_lock);
	}
	else
		Assert(!init);
}


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	int			ring_size_kb;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			ring_size_kb = 256;
			break;
		case BAS_BULKWRITE:
			ring_size_kb = 16 * 1024;
			break;
		case BAS_VACUUM:
			ring_size_kb = 256;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	return GetAccessStrategyWithSize(btype, ring_size_kb);
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
BufferAccessStrategy
GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
{
	int			ring_buffers;
	BufferAccessStrategy strategy;

	Assert(ring_size_kb >= 0);

	/* Figure out how many buffers ring_size_kb is */
	ring_buffers = ring_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

	/* Cap to 1/8th of shared_buffers */
	ring_buffers = Min(NBuffers / 8, ring_buffers);

	/* NBuffers should never be less than 16, so this shouldn't happen */
	Assert(ring_buffers > 0);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_buffers * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->nbuffers = ring_buffers;

	return strategy;
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.
 */
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return 0;

	return strategy->nbuffers;
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->nbuffers)
		strategy->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage of the ring element would have left it
	 * there, but it might've been decremented by clock sweep since then). A
	 * higher usage_count indicates someone else has touched the buffer, so we
	 * shouldn't re-use it.
	 */
	buf = GetBufferDescriptor(bufnum - 1);
	local_buf_state = LockBufHdr(buf);
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
		&& BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
	{
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
{
	if (!strategy)
		return IOCONTEXT_NORMAL;

	switch (strategy->btype)
	{
		case BAS_NORMAL:

			/*
			 * Currently, GetAccessStrategy() returns NULL for
			 * BufferAccessStrategyType BAS_NORMAL, so this case is
			 * unreachable.
			 */
			pg_unreachable();
			return IOCONTEXT_NORMAL;
		case BAS_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BAS_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BAS_VACUUM:
			return IOCONTEXT_VACUUM;
	}

	elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
	pg_unreachable();
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf, bool from_ring)
{
	/* We only do this in bulkread mode */
	if (strategy->btype != BAS_BULKREAD)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!from_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	strategy->buffers[strategy->current] = InvalidBuffer;

	return true;
}
