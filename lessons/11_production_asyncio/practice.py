import asyncio

WORKERS = 3
QUEUE_MAXSIZE = 3                 # bounded Queue：backlog 上界
API_CONCURRENCY = 2               # 3 个 worker 中最多 2 个同时占用 API
QPS = 20                          # 每秒最多启动多少个新 attempt
ATTEMPT_TIMEOUT = 0.2             # 每个 attempt 自己的 time budget
MAX_RETRIES = 2                   # 首次 attempt 之外，最多再 retry 两次
BASE_RETRY_DELAY = 0.05           # 第一次 retry 前的 backoff(退避)  一次失败后，不立刻发起下一次 attempt，而是先等待一段时间
WRITER_CONCURRENCY = 2            # writer 也是有限 resource

class PermanentJobError(Exception):
    """再次立即调用也不会恢复的明确业务失败"""

class RetriesExhaustedError(Exception):
    """可 retry 的失败已经用完全部 attempt"""

def log(event, **fields):
    """structured logging: 事件名 + 明确字段"""
    parts = [f"event={event}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts))

def build_runtime():
    """创建只属于本次service运行周期的控制器、状态与metrics"""
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
            "duplicates": 0, # 重复项
        },
        "processed_ids": set(),
    }

async def wait_for_rate_slot(runtime):
    """让新 attempt 依次等到允许启动的时刻"""
    interval = 1 / QPS  # 均匀分布请求时间
    loop = asyncio.get_running_loop()
    async with runtime["rate_lock"]:
        now = loop.time()
        wait = max(0.0, runtime["next_start"] - now)
        if wait > 0:
            await asyncio.sleep(wait)
        runtime["next_start"] = loop.time() + interval

async def external_api(job, attempt, runtime):
    """模拟外部api调用"""
    if job["id"] in runtime["processed_ids"]:
        runtime["metrics"]["duplicates"] += 1
        log("job 重复项", job_id = job["id"])
        return {"id": job["id"], "duplicate": True}
    behavior = job["behavior"]
    if behavior == "permanent":
        raise PermanentJobError("参数错误")  # permanent failure：不重试
    if behavior == "persistent":
        raise ConnectionError("持续网络故障")
    if behavior == "transient" and attempt == 1:
        raise ConnectionError("网络波动")
    if behavior == "timeout_after_side_effect":
        if attempt == 1:  # 只有第一次调用该任务时才会进入这个分支。如果客户端因为后续收不到响应而发起重试(attempt >= 2) 则不会再次进入此分支，而是会走到函数顶部的幂等检查，直接返回
            runtime["processed_ids"].add(job["id"])
            await asyncio.sleep(1.0)
        return {"id": job["id"]}
    runtime["processed_ids"].add(job["id"])
    await asyncio.sleep(0.05)
    return {"id": job["id"]}

async def call_with_retry(job, runtime):
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            async with runtime["api_gate"]:
                await wait_for_rate_slot(runtime)  
                async with asyncio.timeout(ATTEMPT_TIMEOUT):
                    return await external_api(job, attempt, runtime)

        except TimeoutError:
            reason = "timeout"
        except ConnectionError:
            reason = "transient"
        if attempt <= MAX_RETRIES:
            retry_delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
            runtime["metrics"]["retried"] += 1
            log("job_retry", job_id=job["id"], attempt=attempt, reason=reason, delay=f"{retry_delay:.2f}")
            await asyncio.sleep(retry_delay)
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
            if job is None:
                break
            try:
                result = await call_with_retry(job, runtime)
            except PermanentJobError as error:
                runtime["metrics"]["failed"] += 1
                log("job_failed", worker=name, job_id=job["id"],
                    reason="permanent", error=str(error))
            except RetriesExhaustedError as error:
                runtime["metrics"]["failed"] += 1
                log("job_failed", worker=name, job_id=job["id"],
                    reason="retries_exhausted", error=str(error))
            else:
                await save(result, runtime)
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
        for job in jobs:
            runtime["metrics"]["received"] += 1
            await queue.put(job)
        log("input_closed", received=runtime["metrics"]["received"])
        for _ in range(WORKERS):
            await queue.put(None)  # 往空栈里面放None以关闭workers
        await queue.join()  # graceful shutdown：drain 已接收工作，等待所有已经放进 Queue 的 item 都被标记为处理完成。
    log("shutdown_complete", writer_peak=runtime["writer_stats"]["peak"],
        writer_limit=WRITER_CONCURRENCY)
    print("最终 metrics:", runtime["metrics"])

asyncio.run(main())