# Buffer Pool Eviction Latency Measurement Design

**Commit**: `a34d2e2c347ecd6caf813b123dfc9c22f0044642`
**Author**: yunixxq <2263970991@qq.com>
**Date**: 2026-06-30
**Description**: Add eviction latency measurement for EACLOCK, LIRS, S3FIFO, CLOCK

---

## 1. 实验设计

### 1.1 目标

测量 PostgreSQL buffer pool 置换算法在真实负载下的驱逐延迟（eviction latency），以 P50 / P90 / P99 百分位数衡量。

### 1.2 测试配置

| 参数 | 值 |
|------|-----|
| 数据集 | pgbench scale=200 (~3GB, 2000 万行) |
| 并发 | 10 clients, 10 threads |
| 测试时长 | 每算法 30 秒 |
| Buffer 配置 | 5% (64MB) / 10% (128MB) / 15% (256MB) |
| 负载类型 | 顺序扫描 (`pgbench -S`) |
| 算法 | CLOCK, LIRS, S3FIFO, EACLOCK, WATT |

### 1.3 驱逐触发条件

PostgreSQL 的 buffer 分配遵循 **freelist-first** 策略：

1. 优先从 `StrategyFreeBuffer` 获取空闲 buffer
2. 仅当 freelist 为空时，才调用具体置换算法（`StrategyGetBuffer`）
3. 因此 pgbench 顺序扫描在足够大的数据集下，会强制触发频繁的置换

### 1.4 延迟测量点

在 `StrategyGetBuffer()` 中，选中 victim buffer 并标记为 dirty 后，记录时间戳差值：

```c
instr_time start, end;
INSTR_TIME_SET_CURRENT(start);
// ... eviction algorithm runs ...
INSTR_TIME_SET_CURRENT(end);
INSTR_TIME_SUBTRACT(end, start);
RecordEvictionLatency(data, INSTR_TIME_GET_NS(end));
```

---

## 2. 数据结构

### 2.1 头文件 (`buf_internals.h`)

```c
#define MAX_LATENCY_ALGOS 8
#define MAX_LIRS_SAMPLES 2000000

typedef struct EvictionLatencyData {
    pg_atomic_uint32 sample_count;   // 已采集的样本数
    pg_atomic_uint32 buf_size;       // 当前 NBuffers，用于验证
    uint64 latencies[MAX_LIRS_SAMPLES]; // 延迟样本数组（纳秒）
    slock_t lat_lock;                 // 保护 latencies 数组的自旋锁
} EvictionLatencyData;
```

- 延迟数组上限 200 万样本，远超 30 秒测试的样本量
- `buf_size` 用于标记当前 NBuffers，防止重启后数据错乱
- 使用原子操作 + 自旋锁保证并发安全

### 2.2 全局指针 (`freelist.c`)

```c
EvictionLatencyData *LatencyData_EACLOCK = NULL;
EvictionLatencyData *LatencyData_LIRS = NULL;
EvictionLatencyData *LatencyData_S3FIFO = NULL;
EvictionLatencyData *LatencyData_WATT = NULL;
EvictionLatencyData *LatencyData_ARC = NULL;
EvictionLatencyData *LatencyData_Hyperbolic = NULL;
EvictionLatencyData *LatencyData_LRU2 = NULL;
EvictionLatencyData *LatencyData_CLOCK = NULL;
```

---

## 3. 共享内存初始化

### 3.1 内存布局

所有算法的延迟数据结构一次性分配在同一共享内存段中，通过偏移量定位各自 slot：

```c
void InitEvictionLatency(bool init)
{
    Size total_size = MAX_LATENCY_ALGOS * sizeof(EvictionLatencyData);
    char *base = ShmemInitStruct("Eviction Latency Data", total_size, &found);

    LatencyData_EACLOCK    = (EvictionLatencyData *)(base + 0 * sizeof(EvictionLatencyData));
    LatencyData_LIRS       = (EvictionLatencyData *)(base + 1 * sizeof(EvictionLatencyData));
    LatencyData_S3FIFO     = (EvictionLatencyData *)(base + 2 * sizeof(EvictionLatencyData));
    LatencyData_WATT       = (EvictionLatencyData *)(base + 3 * sizeof(EvictionLatencyData));
    ...
}
```

### 3.2 初始化时机

- `InitEvictionLatency(true)` 在 postmaster 首次启动时调用，分配共享内存并初始化锁
- `InitEvictionLatency(false)` 在每次 `resetlatency` 命令时调用，重新获取指针引用
- `buf_size` 通过 `pg_atomic_write_u32` 强制写入当前 NBuffers，确保重启后数据有效

---

## 4. 延迟记录

### 4.1 记录函数

```c
void RecordEvictionLatency(EvictionLatencyData *data, uint64 ns)
{
    if (data == NULL) return;
    uint32 idx = pg_atomic_fetch_add_u32(&data->sample_count, 1);
    if (idx < MAX_LIRS_SAMPLES) {
        SpinLockAcquire(&data->lat_lock);
        data->latencies[idx] = ns;
        SpinLockRelease(&data->lat_lock);
    }
}
```

- `pg_atomic_fetch_add` 获取槽位索引，保证多后端并发写入不冲突
- 超出数组上限的样本被丢弃（30 秒测试不会超限）

### 4.2 百分位计算

```c
static uint64 compute_percentile(uint64 *arr, uint32 n, double pct)
{
    // 复制到临时数组
    static uint64 sort_tmp[MAX_LIRS_SAMPLES];
    uint32 copy_n = (n < MAX_LIRS_SAMPLES) ? n : MAX_LIRS_SAMPLES;
    memcpy(sort_tmp, arr, sizeof(uint64) * copy_n);
    // 插入排序
    for (uint32 i = 1; i < copy_n; i++) {
        uint64 key = sort_tmp[i];
        uint32 j = i;
        while (j > 0 && sort_tmp[j-1] > key) { sort_tmp[j] = sort_tmp[j-1]; j--; }
        sort_tmp[j] = key;
    }
    return sort_tmp[(uint32)(copy_n * pct / 100.0)];
}
```

---

## 5. 算法实现

### 5.1 CLOCK

`freelist.c` 第 821 行左右。PostgreSQL 原生 CLOCK-Sweep 算法：
- 环形扫描 buffer，依次清除 reference bit
- 找到第一个 `refcount==0 && usage_count==0` 的 buffer 作为 victim

### 5.2 LIRS

`freelist.c` 第 913 行左右。LIRS (Low Inter-reference Recency Set) 算法：
- 维护两个栈：`S`（LIRS 块栈）、`Q`（HIRS 块队列）
- 通过 `node_id` 查找 buffer 在 `lruStack` 中的位置
- 动态调整 LIRS/HIRS 集合，计算 stack distance

### 5.3 S3FIFO

`freelist.c` 第 981 行左右。S3-FIFO 算法：
- 三个队列：`S`（scan‑sensitive）、`M`（main）、`G`（ghost）
- scan‑sensitive 机制：区分顺序扫描和热点访问
- 基于命中率和访问间隔决定是否提升到 M 队列

### 5.4 EACLOCK

`freelist.c` 第 669 行左右。EACLOCK (Enhanced Adaptive CLOCK) 算法：
- 自适应调整扫描步长
- 结合访问频率和最近性计算优先级

### 5.5 WATT

`freelist.c` 第 1047 行左右。WATT (Workload-Adaptive Tree) 算法：
- 维护 Per-Frame 数据结构
- 定期更新 access frequency 和 recency 权重
- 通过 `best_buf_state` 锁保护并发访问

---

## 6. 用户命令接口 (`postgres.c`)

### 6.1 `algorithm X`

切换当前置换算法：

```
postgres=# algorithm c    -- CLOCK
postgres=# algorithm i    -- LIRS
postgres=# algorithm 3    -- S3FIFO
postgres=# algorithm e    -- EACLOCK
postgres=# algorithm w    -- WATT
```

### 6.2 `latency`

输出所有算法的延迟统计：

```
postgres=# latency
LOG:  === Eviction Latency Results ===
LOG:  CLOCK: buf_size=16384 samples=1673394 P50=42 ns P90=167 ns P99=666 ns Max=166625 ns
LOG:  LIRS: buf_size=16384 samples=4833 P50=1267041 ns ...
...
```

### 6.3 `resetlatency`

清零所有算法的延迟数据，并强制更新 `buf_size`：

```
postgres=# resetlatency
```

---

## 7. Benchmark 脚本

`run_eviction_benchmark.sh` 完整流程：

1. **部署 custom postgres**：将编译后的 `postgres` 覆盖原始安装路径
2. **遍历 3 个 Buffer 配置**（64MB / 128MB / 256MB）
3. **每个配置下遍历 5 个算法**：
   - `resetlatency`
   - `algorithm X`
   - `pgbench -c 10 -j 10 -T 30 -S`（30 秒顺序扫描）
   - 解析日志提取 P50/P90/P99
4. **结果输出到 `eviction_benchmark_results.txt`**

LIRS 在小 buffer 下可能触发超时（>90s），脚本会自动跳过。

---

## 8. 编译和执行

### 8.1 编译

```bash
cd /Users/lyx/Projects/bufferpool/EACLOCK-prototype
make -j8
```

### 8.2 初始化数据目录（仅首次）

```bash
rm -rf /tmp/pgtest_data
mkdir -p /tmp/pgtest_data
/opt/LRU-C-PG/pg_install/bin/initdb -D /tmp/pgtest_data
```

### 8.3 执行 Benchmark

```bash
bash /Users/lyx/Projects/bufferpool/EACLOCK-prototype/run_eviction_benchmark.sh
```

脚本会自动部署 custom postgres 到 `/opt/LRU-C-PG/pg_install/bin/`。

---

## 9. 已知问题和限制

### 9.1 LIRS 小 buffer 下极慢

LIRS 在 Buffer=5%（64MB）下因 `node_id` 查找效率低，大量回退到全表扫描，导致 pgbench 事务超时。建议跳过 LIRS 或增大测试时长。

### 9.2 WATT buf_size 修复

早期版本中 `buf_size` 通过 `pg_atomic_init_u32` 设置，仅写入一次。服务器重启后 NBuffers 变化时，`buf_size` 未更新，导致打印逻辑判定为不匹配。修复方法：在 `resetlatency` 命令中使用 `pg_atomic_write_u32` 强制更新 `buf_size`。

### 9.3 延迟测量范围

当前仅测量 `StrategyGetBuffer` 中选 victim 的时间，不包含后续的 dirty write、fsync 等 I/O 操作。驱逐本身在内存中完成（dirty page 由 bgwriter 异步刷盘）。

---

## 10. 文件变更摘要

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `src/backend/storage/buffer/freelist.c` | 修改 | LIRS/S3FIFO/EACLOCK/WATT 实现 + 延迟测量 |
| `src/backend/tcop/postgres.c` | 修改 | latency/resetlatency/algorithm 命令 |
| `src/include/storage/buf_internals.h` | 修改 | `EvictionLatencyData` 结构体定义 |
| `src/backend/storage/buffer/buf_init.c` | 修改 | `buf_size` 初始化 |
| `.gitignore` | 修改 | 忽略编译产物 |
