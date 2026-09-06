# Lesson 11 — 把前面机制组合成长期运行的程序

## 进入本课前

前面几课已经分别回答了这些问题：

- Lesson 03～05：谁负责等待 Task 结束，怎样区分失败、超时和取消，并在退出时收尾；
- Lesson 06～08：怎样限制资源占用和等待量，让处理压力通过 bounded Queue 传回输入端；
- Lesson 09：遇到阻塞式调用时，怎样避免拖住 Event Loop；
- Lesson 10：怎样按业务依赖安排执行顺序，并明确每一步失败后怎么办。

本课沿用这些概念，把它们组合到一个持续接收、处理并保存工作的程序中。重点是：**一条工作从接收到结束，要经过哪些限制；处理失败或程序准备停止时，谁负责把它收尾？**

建议分两遍阅读：

1. 第一遍只追踪“进入 Queue → worker 取出 → 调用外部接口 → 保存结果”，再看输入结束后，程序怎样处理完已接收的工作。
2. 第二遍对照不同工作，分别看重试成功、重试用尽、直接失败，以及“外部状态已改变，但调用超时”的处理方式，最后核对日志和统计数字。

示例只输入 7 条工作，便于运行和核对结果。它演示长期运行程序的核心处理流程，不要求你第一次阅读就记住整份代码。

## 本课新增术语

下面的术语分成三组：怎样停止与恢复、怎样控制资源、怎样观察运行状态。先了解每组解决什么问题，再到示例中查找对应代码即可。

**第一组：停止、失败恢复与重复执行保护**

- **shutdown（关闭流程）**：程序从“还在正常接收和处理工作”走到“停止运行”的整个过程。
- **graceful shutdown（优雅关闭）**：关闭时，先按业务约定处理待完成的工作、释放资源，再退出；本例约定处理完所有已接收的工作。
- **attempt（一次尝试）**：针对同一业务 operation 发起的一次具体调用；retry 会产生新的 attempt。
- **transient failure（暂时性失败）**：过一会儿再试有可能恢复的失败，例如短暂的网络故障；“可能恢复”不保证下一次一定成功。
- **`ConnectionError`**：Python 内置的连接错误异常；本例用它模拟允许有限重试的网络故障。
- **permanent failure（永久性失败）**：在请求内容和业务条件不变时，重复调用也无法解决的失败，例如参数不合法，需要先纠正问题。
- **backoff（退避）**：一次失败后，不立刻发起下一次 attempt，而是先等待一段时间；连续失败时等待通常逐步增加。
- **jitter（随机扰动）**：在 backoff 时间上增加少量随机变化，避免很多 worker 或 service 实例在同一时刻一起 retry。
- **side effect（副作用）**：会改变外部状态的动作，例如写入数据、扣款、发送消息。
- **idempotency（幂等性）**：同一个业务 request 被重复执行时，不会重复产生本不该重复的 side effect。
- **set（集合）**：Python 中只保存不重复元素的容器；本例用它记录哪些 job id 已经产生过 side effect。

**第二组：启动速率、共享状态与资源控制**

- **QPS（Queries Per Second，每秒请求数）**：每秒启动多少次 request 的一种速率表达方式。
- **rate limiter（速率限制器）**：真正执行 rate limit 规则、决定某个新 request 现在能不能启动的控制组件。
- **gate（闸门）**：本课对“进入受限 resource 前必须先获得许可”的控制点的白话称呼。
- **shared state（共享状态）**：多份 Task 都能读写的同一份数据；如果修改步骤会相互打断，结果就可能不正确。
- **`asyncio.Lock()`**：同一时刻只允许一个 Task 进入其保护范围的工具，本课用它保护 rate limiter 的 shared state。
- **`asyncio.get_running_loop()`**：取得当前 coroutine 所在、并且正在运行的 Event Loop；本例先用它取得 Event Loop，再读取用于计算间隔的时间。
- **monotonic clock（单调时钟）**：只用于比较经过时间、不会因为系统日期调整而倒退的时钟；适合计算调度间隔。
- **`loop.time()`**：读取当前 Event Loop 的 monotonic clock 数值；本例用它计算下一次允许启动的时刻。
- **writer（写入器）**：负责把处理结果写入文件或其他存储位置的处理环节。

**第三组：观察运行状态与识别系统风险**

- **counter（计数器）**：只记录某类事件累计发生了多少次的数字。
- **metrics（指标）**：用数字持续记录程序状态，例如收到多少 job、成功多少、失败多少、retry 多少。
- **structured logging（结构化日志）**：用“事件名 + 明确字段”记录日志，让程序可以按字段查询和分析。
- **observability（可观测性）**：通过 metrics、日志等外部信号判断程序内部正在发生什么。
- **task leak（任务泄漏）**：本应结束的 Task 因 lifecycle 管理错误长期残留并继续占用 resource。
- **retry storm（重试风暴）**：大量失败 request 在相近时间集中 retry，反而把已经有压力的 downstream 压得更重。

本例还会用到两种容易看混的普通 Python 写法：函数参数里的 `**fields` 会把额外的“字段名=值”参数收集成一个 dictionary；表达式里的 `2 ** n` 则表示 2 的 n 次方。它们都不是新的 asyncio 机制。

## 一个例子串起全部术语

沿用 Lesson 07 的流水线：输入端把 job 放进 bounded Queue，固定数量的 worker 逐条取出并处理。本课为每条 job 增加两段工作：先调用外部 API，再保存结果。调用失败时按规则重试；输入结束时，处理完已接收的工作再退出。

阅读时可以先从 `main()` 找到 `worker()`，再顺着 `call_with_retry()` 和 `save()` 往下看。`external_api()` 用不同的 `behavior` 模拟成功和失败，`save()` 用等待模拟写入耗时；它们不访问真实外部系统，也不实际保存数据。`build_runtime()` 则集中创建本次运行共用的控制器和状态。完整代码与本课的 `case.py` 一致：

```python
import asyncio

WORKERS = 3
QUEUE_MAXSIZE = 3                 # bounded Queue：backlog 上界
API_CONCURRENCY = 2               # 3 个 worker 中最多 2 个同时占用 API
QPS = 20                          # 每秒最多启动多少个新 attempt
ATTEMPT_TIMEOUT = 0.2             # 每个 attempt 自己的 time budget
MAX_RETRIES = 2                   # 首次 attempt 之外，最多再 retry 两次
BASE_RETRY_DELAY = 0.05           # 第一次 retry 前的 backoff
WRITER_CONCURRENCY = 2            # writer 也是有限 resource

class PermanentJobError(Exception):
    """再次立即调用也不会恢复的明确业务失败。"""

class RetriesExhaustedError(Exception):
    """可 retry 的失败已经用完全部 attempt。"""

def log(event, **fields):
    """structured logging：事件名 + 明确字段，而不是一段自由文本。"""
    parts = [f"event={event}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts))

def build_runtime():
    """创建只属于本次 service 运行周期的控制器、状态与 metrics。"""
    return {
        "api_gate": asyncio.Semaphore(API_CONCURRENCY),
        "writer_gate": asyncio.Semaphore(WRITER_CONCURRENCY),
        "writer_stats": {"active": 0, "peak": 0},
        "rate_lock": asyncio.Lock(),
        "next_start": 0.0,
        "metrics": {
            "received": 0,
            "succeeded": 0,
            "failed": 0,
            "retried": 0,
            "duplicates": 0,
        },
        "processed_ids": set(),  # 已真正产生 side effect 的 job id
    }

async def wait_for_rate_slot(runtime):
    """让新 attempt 依次等到允许启动的时刻。"""
    interval = 1 / QPS
    loop = asyncio.get_running_loop()
    async with runtime["rate_lock"]:
        now = loop.time()
        wait = max(0.0, runtime["next_start"] - now)
        if wait > 0:
            await asyncio.sleep(wait)        # 有意在锁内等：后来的 Task 依次排在后面
        runtime["next_start"] = loop.time() + interval

async def external_api(job, attempt, runtime):
    # side effect 前先做幂等检查：重复执行不能产生重复副作用
    if job["id"] in runtime["processed_ids"]:
        runtime["metrics"]["duplicates"] += 1
        log("job_duplicate", job_id=job["id"])
        return {"id": job["id"], "duplicate": True}
    behavior = job["behavior"]
    if behavior == "permanent":
        raise PermanentJobError("参数错误")       # permanent failure：不 retry
    if behavior == "persistent":
        raise ConnectionError("持续网络故障")      # 看似暂时，但本次一直没有恢复
    if behavior == "transient" and attempt == 1:
        raise ConnectionError("网络抖动")          # transient failure：可 retry
    if behavior == "timeout_after_side_effect":
        if attempt == 1:
            runtime["processed_ids"].add(job["id"])
            await asyncio.sleep(1.0)               # side effect 已发生，但 response 永远等不到
        return {"id": job["id"]}
    runtime["processed_ids"].add(job["id"])
    await asyncio.sleep(0.05)
    return {"id": job["id"]}

async def call_with_retry(job, runtime):
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            async with runtime["api_gate"]:        # 先取得 active concurrency 许可
                await wait_for_rate_slot(runtime)   # 再让真正启动时刻服从 QPS
                async with asyncio.timeout(ATTEMPT_TIMEOUT):
                    return await external_api(job, attempt, runtime)
        except TimeoutError:
            reason = "timeout"
        except ConnectionError:
            reason = "transient"
        if attempt <= MAX_RETRIES:
            retry_delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
            runtime["metrics"]["retried"] += 1
            log("job_retry", job_id=job["id"], attempt=attempt,
                reason=reason, delay=f"{retry_delay:.2f}")
            await asyncio.sleep(retry_delay)       # 下一次 attempt 前先 backoff
    raise RetriesExhaustedError(f"job {job['id']} 重试次数用尽")

async def save(result, runtime):
    async with runtime["writer_gate"]:             # writer 有自己的容量边界
        stats = runtime["writer_stats"]
        stats["active"] += 1
        stats["peak"] = max(stats["peak"], stats["active"])
        try:
            await asyncio.sleep(0.08)               # 模拟比单次 API 启动间隔更慢的写入
        finally:
            stats["active"] -= 1

async def worker(queue, name, runtime):
    while True:
        job = await queue.get()
        try:
            if job is None:                        # sentinel：没有新工作了
                break
            try:
                result = await call_with_retry(job, runtime)
            except PermanentJobError as error:
                runtime["metrics"]["failed"] += 1  # permanent failure：不 retry
                log("job_failed", worker=name, job_id=job["id"],
                    reason="permanent", error=str(error))
            except RetriesExhaustedError as error:
                runtime["metrics"]["failed"] += 1
                log("job_failed", worker=name, job_id=job["id"],
                    reason="retries_exhausted", error=str(error))
            else:
                await save(result, runtime)         # writer 未知失败不会伪装成 API 失败
                runtime["metrics"]["succeeded"] += 1
        finally:
            queue.task_done()

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    runtime = build_runtime()
    async with asyncio.TaskGroup() as tg:
        for worker_number in range(WORKERS):
            tg.create_task(worker(queue, f"worker-{worker_number}", runtime))
        jobs = [
            {"id": 1, "behavior": "ok"},
            {"id": 2, "behavior": "ok"},
            {"id": 3, "behavior": "ok"},
            {"id": 4, "behavior": "timeout_after_side_effect"},
            {"id": 5, "behavior": "transient"},
            {"id": 6, "behavior": "persistent"},
            {"id": 7, "behavior": "permanent"},
        ]
        for job in jobs:                            # 停止接收新输入之前只到这里
            runtime["metrics"]["received"] += 1
            await queue.put(job)                    # Queue 满 → 生产侧自动放慢
        log("input_closed", received=runtime["metrics"]["received"])
        for _ in range(WORKERS):
            await queue.put(None)
        await queue.join()                          # graceful shutdown：drain 已接收工作
    log("shutdown_complete", writer_peak=runtime["writer_stats"]["peak"],
        writer_limit=WRITER_CONCURRENCY)
    print("最终 metrics:", runtime["metrics"])

asyncio.run(main())
```

一次运行可能看到下面的日志；并发下相邻事件的顺序和具体 `worker-*` 名称可以略有变化，但最终 counters 应保持一致。由于 bounded Queue 会让 producer 与 worker 交替推进，早期 `job_retry` 甚至可能出现在 `input_closed` 之前；这只表示 worker 已开始处理，而 producer 还在放入后续 job：

```text
event=input_closed received=7
event=job_retry job_id=5 attempt=1 reason=transient delay=0.05
event=job_retry job_id=6 attempt=1 reason=transient delay=0.05
event=job_retry job_id=4 attempt=1 reason=timeout delay=0.05
event=job_retry job_id=6 attempt=2 reason=transient delay=0.10
event=job_duplicate job_id=4
event=job_failed worker=worker-1 job_id=7 reason=permanent error=参数错误
event=job_failed worker=worker-2 job_id=6 reason=retries_exhausted error=job 6 重试次数用尽
event=shutdown_complete writer_peak=2 writer_limit=2
最终 metrics: {'received': 7, 'succeeded': 5, 'failed': 2, 'retried': 4, 'duplicates': 1}
```

把本课知识点对到代码上：

| 术语或知识点 | 在这个例子里指什么 |
| --- | --- |
| **shutdown** | `main()` 从停止产生新 job、发送 sentinel，一直走到所有 worker 结束的完整停止过程 |
| **graceful shutdown** | `await queue.join()` 先 drain 已接收 job，再离开 `TaskGroup`，而不是直接丢下仍在处理的工作 |
| **attempt** | `for attempt in range(...)` 的每次循环都是同一 job 的一次具体外部调用 |
| **示例输入** | `jobs` 中的 `behavior` 指定每条工作的模拟行为；`persistent` 表示连接故障一直没有恢复，与参数错误 `permanent` 不同 |
| **transient failure** | Job 5 下一次 attempt 恢复；job 6 虽被分类为可 retry 的连接故障，但本次始终没有恢复 |
| **`ConnectionError`** | `external_api()` 用它模拟 job 5 与 job 6 的连接故障；只有这个已分类类型进入 transient retry 路径 |
| **permanent failure** | Job 7 触发专用的 `PermanentJobError("参数错误")`，worker 直接记录失败而不 retry |
| **backoff** | 每次允许 retry 时先等待 `0.05 × 2^(attempt-1)` 秒，job 6 的两次等待依次约为 0.05 和 0.10 秒 |
| **jitter** | 为保持课程输出与时长稳定，最小例子没有加入随机变化；真实多实例 service 通常还要避免同步 retry |
| **side effect** | `runtime["processed_ids"].add(job["id"])` 代表外部状态已经被真正改变 |
| **idempotency** | Retry 前检查稳定的 job id；job 4 首次已产生 side effect，第二次只记录 duplicate，不重复执行 |
| **set** | `runtime["processed_ids"]` 是只保存唯一 job id 的集合，支持 `id in ...` 检查与 `.add(id)` 登记 |
| **QPS** | `QPS = 20` 表示每秒最多为约 20 次新 attempt 安排启动时刻 |
| **rate limiter** | `wait_for_rate_slot()` 根据 QPS 让 attempt 依次等到允许启动的时刻，相邻放行时刻至少相隔一个 `interval` |
| **gate** | 3 个 worker 共用容量为 2 的 `api_gate`，所以 worker 数不等于 API 容量；`writer_gate` 另行守住写入 resource |
| **shared state** | 所有 worker 共用 `runtime["next_start"]`，每次安排 attempt 都必须读写它 |
| **`asyncio.Lock()`** | `runtime["rate_lock"]` 一次只让一个 Task 负责“等到自己的时刻并推进下一时刻”；这里的等待有意位于锁内 |
| **`asyncio.get_running_loop()`** | `wait_for_rate_slot()` 用它取得当前 coroutine 正在使用的 Event Loop，并保存到 `loop` |
| **monotonic clock / `loop.time()`** | `loop.time()` 提供只用于计算间隔的时间值，不把系统日期当调度依据 |
| **writer** | `save(result, runtime)` 代表较慢写入；`writer_gate` 限制同时写入数量，`writer_stats` 实测 peak |
| **counter / metrics** | 正常收尾后用 `received = succeeded + failed` 核对最终结果；`retried` 与 `duplicates` 记录过程事件，`writer_stats["peak"]` 记录资源占用峰值 |
| **structured logging** | `log()` 输出输入关闭、retry、失败、duplicate 和 shutdown 完成等事件；retry 还带有等待时长字段 |
| **额外日志字段** | `log(event, **fields)` 把 `job_id=...`、`reason=...` 等命名字段收集成 dictionary，随后稳定地输出每个字段 |
| **observability** | 日志指出具体 job 的路径，最终 metrics 给出总量，两者组合后才能解释结果 |
| **task leak** | 正常路径中没有 task leak：workers 都由 `TaskGroup` 拥有，并在函数返回前收到 sentinel、完成并结束 |
| **retry storm** | 固定 worker、rate limiter 与 `MAX_RETRIES` 给 retry 压力建立边界；metrics 与日志负责暴露异常增长 |
| **runtime ownership** | `build_runtime()` 在 `main()` 运行后创建 gates、Lock、状态和 metrics，避免下一次运行继承上次的可变全局状态 |
| **失败分类** | `PermanentJobError` 与 `RetriesExhaustedError` 分别表达两条已知 API 路径；writer 位于这些 `except` 之外，未知写入错误不会被误报成 API 业务失败 |

按时间线沿一条 job 的执行路径和整体 shutdown 读取：

1. `main()` 创建容量为 3 的 Queue 和 3 个长期 worker；输入更快时，`queue.put()` 会把 backpressure 传回生产侧。
2. Worker `get()` 一条 job 后调用 `call_with_retry()`，第一次循环就是 attempt 1。
3. 最多两个 worker 能同时通过 `api_gate`；attempt 取得许可后再由 rate limiter 等到允许启动的时刻，最后只给真正的外部调用套上 timeout。
4. 普通 job 调用成功后，worker 通过 `writer_gate` 模拟保存结果，返回后才增加 `succeeded`。代表性输出中的 `writer_peak=2` 表示最多有两次写入重叠；具体峰值取决于调度，但不应超过 `writer_limit`。
5. Job 5 第一次遇到 transient failure，记录 backoff 时长并等待后，下一次 attempt 成功。
6. Job 6 的连接故障连续出现；首次 attempt 之外只允许两次 retry，三次都失败后记录 `retries_exhausted`，证明循环有终点。
7. Job 4 第一次已经登记 side effect，但等待 response 时 timeout；retry 时幂等检查识别相同 job id，只返回 duplicate 结果并增加 counter。
8. Job 7 遇到 permanent failure，不进入 retry 分支，直接记录 `job_failed`。
9. 每次成功 `get()` 后，worker 都在 `finally` 中配对调用 `task_done()`，包括取到 sentinel 时。正常路径下，这表示当前条目已处理结束；它不表示每条 job 都成功了。
10. 输入结束时先记录 `input_closed`，再为每个 worker 放入一个 sentinel，并等待 Queue drain。
11. 三个 worker 全部结束后 `TaskGroup` 才退出并记录 `shutdown_complete`；日志同时报告 writer peak 与 limit，最终 metrics 是 7 条接收、5 条成功、2 条失败、4 次 retry、1 次 duplicate。

## 本节目标

学完本节，你应该能够：

- 设计 graceful shutdown，并明确什么时候需要 drain；
- 把 concurrency limit 与 rate limit 同时放进一个长期运行程序；
- 为每个外部调用 attempt 设置 timeout，并限制 retry 条件与次数；
- 区分 transient failure 与 permanent failure；
- 解释 idempotency 为什么能保护重复执行；
- 限制 writer 的 resource 容量；
- 解释 rate limiter 为什么要保护共享的下一次启动时间；
- 使用 metrics 与 structured logging 建立基本 observability；
- 识别 task leak、变慢的 downstream 和 retry storm 的信号。

## 为什么需要学习它

前面各课分别解决了任务归属、超时和容量问题。把它们放进同一个程序后，还要考虑它们怎样相互影响。例如，缺少合理限制时可能出现：

```text
downstream 变慢
    ↓
等待处理的工作增多
    ↓
调用等待变久，timeout 增多
    ↓
retry 增多
    ↓
downstream 压力更大
```

Lesson 07 的 bounded Queue 能限制队列中的等待量，并让输入端放慢，但不能让下游自动恢复。本课还要给重试设定边界，防止失败后的额外调用继续加重压力。

此外，程序要能回答：重复调用会不会重复改变外部状态？写入跟不上时，工作在哪里等待？停止接收输入后，已经接收的工作怎么办？出问题时，从哪里看出原因？

最后一课的目标，就是把前面已经学过的独立机制组合成一个可解释的整体模型。

## 核心理论

### 1. 沿用前课模型，先画出整条处理流程

```text
输入
 ↓
bounded Queue
 ↓
固定数量 worker
 ↓
API concurrency gate + rate limiter
 ↓
外部 API
 ↓
writer concurrency gate
 ↓
结果存储
```

和 Lesson 10 一样，先确认依赖：同一条 job 必须先取得 API 结果，才能保存结果。因此，这两步在同一个 worker 中顺序 `await`，不需要各创建一个 Task。不同 worker 处理的 job 则可以交错推进。

再沿用 Lesson 06～08 的容量模型：Queue 限制尚未取出的工作数量，worker 数限制同时处理多少条 job，两个 gate 分别限制 API 阶段和写入阶段。三个 worker 共用两张 API 通行证，因此 worker 数不等于 API 并发数。

最后确认归属：`main()` 创建 Queue，并通过 `build_runtime()` 创建本次运行的 Semaphore、Lock、共享状态和指标，再显式传给 worker。所有 worker 由同一个 `TaskGroup` 负责。这样重复调用主流程时，每次都会获得独立状态，不会继承上次的计数和处理记录。

### 2. Concurrency limit 与 rate limit 同时存在

前面已经分别学过：

- concurrency limit 控制同一时刻正在进行多少调用；
- rate limit 控制单位时间允许启动多少新调用。

长期运行程序里常常两个都需要。

例如：

```text
API concurrency = 5
QPS = 10
```

意思是：

- 同一时刻最多 5 个 API attempt 正在进行；
- 每秒最多启动 10 个新 attempt。

一个限制“同时占用量”，一个限制“启动速度”。

本课的 rate limiter 还需要安全安排“下一次允许启动的时间”。多份 Task 会竞争同一条启动时间线，所以 `wait_for_rate_slot()` 用 `asyncio.Lock()` 让它们逐个排队：

```python
loop = asyncio.get_running_loop()
async with runtime["rate_lock"]:
    now = loop.time()
    wait = max(0.0, runtime["next_start"] - now)
    if wait > 0:
        await asyncio.sleep(wait)
    runtime["next_start"] = loop.time() + interval
```

`asyncio.get_running_loop()` 先取得正在执行 `wait_for_rate_slot()` 的 Event Loop，后面的 `loop.time()` 再读取它的单调时钟。这里关心的是“经过了多久”，不需要读取当前日期和时间。

这里 `interval = 1 / QPS`。本例 `QPS = 20`，所以每次放行后，把下一次允许放行的时刻设为至少 0.05 秒以后。

为什么需要 Lock？假设两个 Task 都读到同一个 `next_start`，然后各自等待到那个时刻，它们就可能一起启动。Lock 把“读取时刻 → 等待 → 更新下一时刻”作为一个整体，一次只允许一个 Task 执行。

因此，这次 `sleep()` **有意放在锁内**：拿到锁的 Task 先等到自己的启动时刻，更新 `next_start` 后释放锁，后一个 Task 再计算自己的等待时间。等待期间 Event Loop 仍能调度其他工作，只是其他需要这把锁的 Task 必须排队。若当前 Task 在 `sleep()` 时被取消，`async with` 会释放锁，也不会提前写入一个尚未使用的未来时刻。

这里要保护的是整段启动安排，所以把等待也放在锁内。其他共享状态是否需要同样处理，应根据具体读写步骤判断，不能照搬锁的范围。

本例还把顺序写成：

```python
async with runtime["api_gate"]:
    await wait_for_rate_slot(runtime)
    async with asyncio.timeout(ATTEMPT_TIMEOUT):
        ...
```

这里的顺序是“先拿 API 通行证，再等待允许启动的时刻，最后开始调用”。这样限速器放行后，不会再因为等待通行证而推迟启动。

如果交换顺序，多条已被限速器放行的调用可能堵在 Semaphore 外，随后又在通行证可用时集中启动。本例选择的代价是：等待 Lock 或启动时刻时，也会占用 API 通行证。因此，`api_gate` 限制的范围包含这段等待，实际正在调用 API 的数量可能更少。

### 3. 每个 attempt 都要有自己的 timeout

假设一次业务 operation 最多 retry 2 次。

可能出现：

```text
attempt 1 → timeout
attempt 2 → transient failure
attempt 3 → 成功
```

每个 attempt 的真实外部调用都应该有明确 timeout，否则其中一次调用可能无限等下去，导致整个 retry 策略失去边界。

所以：

```text
有限 retry 次数
    +
每次 attempt 有 timeout
    =
为外部调用的等待设置边界
```

沿用 Lesson 05 的规则，timeout 约束的是它包住的代码范围。本例的 0.2 秒只覆盖 `external_api()`，不包含入队和排队、等待通行证、等待启动时刻、重试前的 backoff，以及后续写入。

因此，最多调用三次、每次 timeout 为 0.2 秒，不能推出整条 job 会在 0.6 秒内结束。如果业务要求从接收到完成也有总时限，还需要为整个 operation 单独设计时间边界。Timeout 依靠协作式取消生效，也不是无论内部代码怎样执行都能准时终止的硬保证。

### 4. 只对明确的失败类型 retry

Retry 不能写成：

```python
except Exception:
    retry()
```

更合理的思路是先分类：

```text
transient failure     → 可能适合 retry
permanent failure     → 通常不 retry
调用者发来的 cancellation → 不应当 retry
```

本课的原则是：

> retry 必须有适用条件和次数上限。

沿用 Lesson 10 的做法，把失败含义落实到明确异常类型上：

| 调用结果 | 本例的处理方式 |
| --- | --- |
| `ConnectionError` | 视为可能恢复的连接故障，有剩余次数时重试 |
| `TimeoutError` | 在本例具备重复执行保护的前提下，允许有限重试 |
| `PermanentJobError` | 请求本身有问题，直接交给 worker 记录失败 |
| 重试次数用尽 | 抛出 `RetriesExhaustedError`，由 worker 记录失败 |
| 调用者取消或未知异常 | 不在这里转成普通失败，继续向外传播 |

这也延续了 Lesson 05 的结论：超时本身并不说明可以重试。本例为什么允许重试超时调用，要结合下一节的幂等性一起理解。

`MAX_RETRIES = 2` 表示首次 attempt 之外最多再试两次，所以最多有三个 attempts。示例中的 job 6 连续三次连接失败，第三次后不会再打印 retry，而是明确进入 `retries_exhausted` 失败路径。

“Retry 已用尽”本身也是一种明确的失败分类，所以例子使用专用的 `RetriesExhaustedError`。Worker 只捕获这个类型来记录 `retries_exhausted`；如果代码内部意外抛出别的 `RuntimeError`，它不会被伪装成正常的业务失败。越接近长期运行的 service，异常类型越应该表达清楚“谁能处理它”。

即使允许 retry，也不要在失败后立刻紧密重发。本例使用简单的指数 backoff：

```text
第一次 retry 前 → 等待 0.05 秒
第二次 retry 前 → 等待 0.10 秒
```

等待逐步增加，给可能正在恢复的 downstream 留出时间。真实系统中，很多 service 实例可能在同一时刻失败；如果它们都使用完全相同的 backoff，就可能再次同时醒来。因此生产策略通常还会加入 jitter，把启动时刻稍微打散。

为了让课程运行时长和输出稳定，`case.py` 没有使用随机数实现 jitter，但这不表示生产系统可以忽略它。Rate limiter、有限 retry、backoff 和 jitter 解决的是彼此相关但不同的压力来源。

### 5. Retry 会带来重复执行，所以要考虑 idempotency

假设第一次 attempt 实际已经完成 side effect，只是 response 在 network 途中丢失。

调用者看到 timeout 后 retry：

```text
attempt 1
└─ 已经写入成功
   └─ response 丢失

attempt 2
└─ 再次写入
```

如果没有 idempotency，就可能产生重复数据、重复扣款或重复消息。

常见做法是给业务 request 一个稳定标识，例如 `job_id`，并在真正产生 side effect 前检查是否已经处理过。

但要注意：

> 只有 `job_id` 字段本身不会自动产生 idempotency；代码必须真的用它阻止重复 side effect。

对照 job 4 看：第一次调用先把 id 加入 `processed_ids`，表示副作用已经发生，随后等待 1 秒来模拟迟迟未返回的响应。外层 0.2 秒的 timeout 先到期，所以调用方没有拿到结果。第二次调用仍使用同一个 id，检查发现已经处理过，就返回 `duplicate` 结果，避免再次产生副作用。

本例在同一个 Event Loop 中执行，检查 id 与登记 id 之间没有 `await`，所以这段模拟检查不会被另一个 worker 在中间打断。但内存 `set` 只适合演示：程序重启后记录会丢失，多个进程也不会自动共享这份集合。

真实系统要让幂等记录在重启后仍可用，并保证检查、执行业务动作和记录结果相互配合，避免两个相同请求同时通过检查。调用方还必须确认外部接口确实支持这样的保护；只在本地保留一个 id，不能保证外部扣款或发送消息只发生一次。

### 6. Writer 也有 resource 容量

很多程序只限制外部 API，却忘了最终写入同样可能成为瓶颈。

因此：

```text
很多处理结果
    ↓
writer concurrency gate
    ↓
有限写入 resource
```

如果 writer 太慢，仍然可能导致 upstream backlog 增长。

所以 resource 模型要覆盖整条 pipeline，而不是只盯住外部调用。

本例复用 Lesson 06 的 `active / peak / finally` 观测方式：进入写入范围时增加 `active`，离开时在 `finally` 中减回去，并记录运行中观察到的最大值 `peak`。代表性输出 `writer_peak=2 writer_limit=2` 表示这次运行确实出现了两次写入重叠；验收上限时应检查 `peak <= writer_limit`，不能要求每种调度下峰值都恰好等于 2。

本例由 worker 自己等待 `save()` 返回，因此写入变慢会让 worker 更晚取下一条 job，进而让 Queue 填满、输入端等待。这就是 Lesson 07 的反压沿整条流水线向上传递。

失败边界也要按 pipeline 阶段区分。本例只在 `call_with_retry()` 周围捕获 `PermanentJobError` 与 `RetriesExhaustedError`；`save()` 放在这个 `except` 范围之外。于是一个未知 writer 错误会让 `TaskGroup` 明确失败，而不会被错误计入“API permanent failure”。真实业务也可以为 writer 设计 retry 或隔离策略，但必须单独决定，不能复用 API 的错误分类假装已经处理。

### 7. Graceful shutdown 先写业务承诺

本课采用的 shutdown 策略是：

```text
停止接收新输入
    ↓
发送每个 worker 各自的 sentinel
    ↓
worker 继续处理已接收 job，完成后登记 task_done()
    ↓
queue.join() 返回，并等待所有 worker 结束
    ↓
关闭 client / writer 等 resource
    ↓
返回最终 metrics
    ↓
程序结束
```

这延续了 Lesson 07 的 drain：停止新增工作后，把已接收的工作处理完。`queue.join()` 只是等待完成登记，真正处理 job 的仍然是 worker；离开 `TaskGroup` 则确保这些 worker 都已结束。

这里的“处理完”包括成功保存结果，也包括明确记录已知失败，不要求每条 job 都成功。若出现未知异常或上层取消，本例会沿 Lesson 03～04 的规则退出：任务收尾，失败或取消继续向外传播，不再承诺完成正常 drain，也不会打印正常的 `shutdown_complete`。`task_done()` 在这条路径上仅配对本次 `get()`，不能拿来证明业务成功。

Graceful shutdown 要先明确哪些工作承诺处理完、哪些情况允许停止，再安排收尾顺序。若业务只允许等待固定时长，还要另行约定超过关闭时限后如何处理未完成工作。

`case.py` 使用有限的 7 条 job，把“输入结束”当作 shutdown 触发点，所以可以稳定运行和观察。真实长期 service 还需要把操作系统信号、服务框架关闭通知或管理员命令转换成“停止接收新输入”；这属于接入环境的边界，不在这个最小核心示例里伪造。

本例的 `external_api()` 与 `save()` 都是用 `asyncio.sleep()` 模拟的，因此没有真实 HTTP session、数据库连接池或文件句柄需要关闭。把它换成真实 client / writer 后，应当由 `main()` 或一个更外层的 service lifecycle 用 `async with` 拥有这些 resource，并在 Queue drain、worker 结束后关闭；不能因为示例里的模拟对象无需关闭，就省略真实 resource 的 cleanup 设计。

有些程序可能选择立即停止剩余工作；那也是一种 shutdown 策略，但必须由业务承诺决定，而不是随手实现。

### 8. Metrics 让程序状态可以量化

最基础的 counter 可以包括：

```text
received
succeeded
failed
retried
duplicates
```

这些只是字段名，例如 `retried` 这个 counter 表示“累计发生过多少次 retry”。

不同 counter 统计的对象并不相同。本例正常处理完全部输入后，最终结果满足：

```text
received = succeeded + failed
       7 = 5 + 2
```

这里成功的 5 条是 job 1～5，失败的 2 条是 job 6 和 7。`retried=4` 来自 job 4、5 各重试一次，job 6 重试两次；`duplicates=1` 来自 job 4 第二次调用命中幂等检查。这些过程事件已经发生在那 7 条 job 中，不能再加到接收总数里。

再细看计数位置：`retried` 在 backoff 之前增加，严格说记录的是“已决定安排一次重试”，不保证取消发生后那次调用仍会启动。`received` 则在 `queue.put()` 之前增加，所以中途取消时也不能直接把它当成成功入队数。上面的等式用于本例正常收尾后的核对，不是任意时刻都成立的关系。

如果：

```text
retried 很快上涨
failed 也上涨
```

可能说明 downstream 正在持续失败，并且 retry 正在增加额外压力。

`case.py` 中的 dictionary counters 只用于单次进程内演示：程序重启后会清零，也不会自动汇总其他 service 进程。真实部署通常把 counters 交给专门的指标存储与汇总系统，并谨慎选择维度；像每个 `job_id` 这样取值数量可能无限增长的信息更适合放进日志，而不是给 metrics 制造海量独立标签。

### 9. Structured logging 让单个事件可追踪

与只写：

```text
retrying
```

相比，更有用的是记录：

```text
event=job_retry job_id=123 attempt=2 reason=timeout
```

这里的 `event`、`job_id`、`attempt`、`reason` 都只是日志字段名。

`def log(event, **fields)` 中的 `**fields` 会把调用时额外写下的命名字段收进一个 dictionary。例如 `log("job_retry", job_id=5, reason="timeout")` 进入函数后，`fields` 就保存 `job_id` 和 `reason`；随后统一输出“字段名=值”。这让不同事件可以携带不同字段，又保持同一个日志入口。

本例用 `print()` 让学习者不安装日志系统也能观察字段；真实 service 通常交给结构化日志工具输出机器可解析格式，并由日志系统补充时间、等级、service 实例等公共字段。教学重点是事件名与字段含义稳定，不是把 `print()` 当作生产日志基础设施。

这样日志里能直接回答：

- 哪个 job？
- 第几次 attempt？
- 为什么 retry？

### 10. Observability 是为了从外部判断内部问题

如果出现 retry storm，可以观察：

- retry metrics 快速上涨；
- downstream 的失败同时增加；
- Queue 经常达到容量上限，输入端等待变久；
- structured logging 中出现大量相似 retry 事件。

如果出现 task leak，可以观察：

- 已完成业务数量稳定，但存活 Task 数持续上升；
- 程序准备 shutdown 时总有本应结束的 Task 残留；
- 某些 Task 已经没有明确 owner。

Observability 的目标不是“日志越多越好”，而是：

> 关键业务状态和 resource 压力，能否从外部信号中被看见。

## 脑内执行模型

正常运行：

```text
运行中
接收输入
    ↓
Queue
    ↓
workers
    ↓
API gate + rate limiter
    ↓
writer gate
    ↓
结果已保存
```

shutdown：

```text
停止中
停止接收新输入
    ↓
drain 已接收工作
    ↓
workers 结束
    ↓
resources 关闭
    ↓
得到最终 metrics
    ↓
已停止
```

失败恢复：

```text
attempt
  ├─ 成功                     → 保存结果，再记录 job 成功
  ├─ 连接故障或允许重试的超时   → 释放 API 通行证
  │                            ├─ 还有次数 → backoff → 重新申请通行证和启动时刻
  │                            └─ 次数用尽 → 记录 job 失败
  ├─ permanent failure        → 不重试，记录 job 失败
  └─ cancellation 或未知异常   → 收尾并向外传播
```

## 常见误解

- **误区：** QPS=10 就等于 concurrency=10。  
  **更准确：** QPS 表达启动速率；concurrency 表达同时进行的数量。

- **误区：** 失败就无限 retry 能提高成功率。  
  **更准确：** 这可能形成 retry storm，并放大 downstream 的压力。

- **误区：** 只要限制重试次数，整条 job 就一定能及时结束。
  **更准确：** 单次调用也需要 timeout；排队、backoff 和写入等耗时还要另行考虑。

- **误区：** 有 `job_id` 就天然具备 idempotency。  
  **更准确：** 实现必须真的利用稳定标识避免重复 side effect。

- **误区：** graceful shutdown 就是对所有 worker 立刻发 cancellation。  
  **更准确：** 是否 drain 已接收工作取决于业务承诺。

- **误区：** 只限制外部 API concurrency 就够了。  
  **更准确：** writer 和其他有限 resource 同样可能成为瓶颈。

- **误区：** metrics 只统计成功数即可。  
  **更准确：** 至少还要能看到失败、retry、重复和 backlog 等关键状态。

- **误区：** 最终打印的所有 counters 都应该相加等于 `received`。
  **更准确：** `succeeded` / `failed` 是互斥结果；`retried` / `duplicates` 是过程中可与结果重叠的事件。

- **误区：** structured logging 就是写更多字符串。  
  **更准确：** 关键是事件名和字段结构稳定、可查询。

- **误区：** Worker 捕获宽泛的 `ValueError` / `RuntimeError`，再统一猜成某种业务失败最省事。
  **更准确：** 宽泛捕获会把 API、writer 或程序缺陷混在一起；应只在对应阶段捕获表达已知结果的专用异常，让未知错误继续暴露。

## 本节规则总结

1. 先画依赖和容量：同一条 job 顺序调用、保存，不同 worker 并发处理；Queue、API 和 writer 分别有自己的上限。
2. 并发限制控制同时占用量，速率限制控制启动速度；首次调用和重试都要经过这两道限制。
3. Lock 保护完整的共享状态操作；本例把读取、等待和更新启动时刻放在同一个锁内。
4. 单次调用需要 timeout，整条 job 的总时限要另行设计。
5. 只重试明确允许的失败，限制次数，并在重试前 backoff；多实例场景还要考虑 jitter。
6. 重试前确认重复执行安全。幂等性需要实际阻止重复副作用，不能只靠一个 id 字段。
7. 沿用前课的明确异常分类：已知失败在对应阶段处理，未知错误和取消继续向外传播。
8. 正常关闭时，先停止输入并 drain，再等待 worker 结束，最后关闭真实资源；完成登记不等于业务成功。
9. 指标说明总体数量，日志解释具体事件；区分 job 的最终结果和重试等过程事件。
10. Task 和共享状态都要有明确归属。真实部署还需补齐资源关闭、持久化幂等记录，以及跨进程的指标和日志收集。

## 关键问题

1. shutdown 与 graceful shutdown 有什么区别？
2. attempt 与 retry 的关系是什么？
3. transient failure 与 permanent failure 有什么区别？
4. backoff 与 jitter 分别解决什么问题？
5. side effect 在本课里指什么？
6. idempotency 为什么不能只靠“调用者不要重复发送”？
7. QPS 与 concurrency limit 分别控制什么？
8. rate limiter 负责什么？
9. gate 在本课里表示什么？
10. 为什么每次 attempt 自己仍要有 timeout？
11. writer 为什么也需要资源上限？怎样用输出中的 `writer_peak` 和 `writer_limit` 检查写入并发？
12. counter 与 metrics 有什么关系？
13. graceful shutdown 与 drain 的关系是什么？
14. metrics 与 structured logging 分别提供什么信息？
15. observability 的目标是什么？
16. 哪些信号会让你怀疑出现 retry storm？
17. 哪些现象会让你怀疑存在 task leak？
18. 为什么本例的 rate limiter 有意把 `sleep()` 放在 `asyncio.Lock()` 内？它换来了什么，又让其他 Task 在哪里等待？
19. `MAX_RETRIES = 2` 最多会产生几个 attempts？
20. 为什么本例先取得 API gate，再等待 rate slot？交换顺序可能带来什么现象？
21. 为什么每次 attempt 有 timeout，仍不能推出整个 job 一定在同样时间内结束？
22. 内存 `set` 为什么不足以实现跨进程、跨重启的生产 idempotency？
23. 为什么 `PermanentJobError` / `RetriesExhaustedError` 比捕获所有 `ValueError` / `RuntimeError` 更能保护错误分类？为什么 writer 位于这些 `except` 之外？
24. `def log(event, **fields)` 与表达式 `2 ** n` 中的 `**` 分别是什么意思？
25. 为什么本例使用 3 个 worker，却只给 API gate 2 张通行证？
26. 为什么正常收尾后 `received = succeeded + failed`，却不能再加上 `retried` 与 `duplicates`？中途取消时，这个等式还一定成立吗？

## 场景命题

实现一个 `Job Processing Service`。

这个练习名表示“持续接收 job、处理并保存结果的 service”。

程序必须明确：

- bounded Queue；
- 固定数量 worker；
- API concurrency limit；
- QPS / rate limit；
- 每个 attempt 的 timeout；
- 有限 retry；
- retry 用尽使用明确的失败类型，不把未知异常误分类；
- retry backoff，并说明多实例时 jitter 的作用；
- `job_id` idempotency；
- writer concurrency limit，并用实际 peak 验证；
- graceful shutdown + drain；
- structured logging 与 metrics。

练习至少覆盖这些可观察路径：

- 一个普通 job 首次成功；
- 一个 transient failure 在有限 retry 后恢复；
- 一个 transient failure 用尽所有 retry 后失败；
- 一个 permanent failure 完全不 retry；
- API permanent failure、retry 用尽与 writer 未知失败必须是三条不同路径；
- 一个 side effect 已发生但 response timeout 的 job，通过 idempotency 避免重复副作用；
- 输入关闭后先 drain，所有 worker 都结束，最后才打印 shutdown 完成；
- 最终 metrics 能与逐条结构化日志互相核对。

实现前先画出 Queue、worker、API gate、rate limiter、writer gate 的顺序，并明确每个可变对象由哪一层创建和管理。实现后在同一个进程中连续调用主流程两次，确认第二次不会继承第一次的指标、幂等记录或限速器时间状态；仅重新启动脚本，不能验证进程内是否残留了全局状态。

可以直接复用 `case.py` 中的 `external_api()` 模拟函数和 `jobs` 数据，不必重新编写故障模拟。先只保留普通成功输入，之后逐项加入失败场景。

建议按下面的关卡推进，每关都能独立运行并退出：

| 关卡 | 本次增加什么 | 运行后检查什么 |
| --- | --- | --- |
| 1 | 复用 Lesson 07 的 bounded Queue、固定 worker、sentinel、`task_done()` 和 `join()` | 普通 job 全部处理完，worker 全部结束 |
| 2 | 在 worker 内顺序调用 API 和 writer，并给两者设置 Semaphore | 保存发生在 API 返回后，写入峰值不超过上限 |
| 3 | 加入 rate limiter | 每次 API 启动前都经过限速等待 |
| 4 | 加入单次调用 timeout | 超时能向外报告，不能挂住整个示例 |
| 5 | 加入失败分类、有限 retry 和 backoff，并启用幂等检查 | 逐项加入 transient、persistent、permanent 和副作用后超时场景，核对各自结果 |
| 6 | 汇总结构化日志与指标，检查完整关闭流程 | 输入关闭后继续处理，所有 worker 结束后才报告关闭完成，正常运行的数字能核对 |

每一关先确认新增行为，再进入下一关。最后单独让 writer 抛出一次未知异常，确认它使任务组失败，而不是被记成 API 业务失败。第一关已有正常退出流程，最后一关是在此基础上核对关闭承诺和观察信号。

---

完成本课后：回到 [Course Map](../../COURSE_MAP.md) 复盘整条路线，并选择仍无法用自己的话解释的 Lesson 重新运行 `case.py`。
