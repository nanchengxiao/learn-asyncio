import asyncio

LIMIT = 3  # 并发限制
 
async def call_downstream(item, stats):
    stats['active'] += 1
    stats['peak'] = max(stats['active'], stats['peak'])
    try:
        await asyncio.sleep(0.1)
        return item * 10
    finally:
        # 即使失败或cancellation，也不能让观测值永远多算一份 active 工作
        stats['active'] -= 1

async def process(item, semaphore, stats):
    async with semaphore:
        return await call_downstream(item, stats)

async def main():
    semaphore = asyncio.Semaphore(LIMIT)
    stats = {'active': 0, 'peak': 0}
    tasks = []
    async with asyncio.TaskGroup() as tg:
        for item in range(10):
            tasks.append(tg.create_task(process(item=item, semaphore=semaphore, stats=stats)))
    print([task.result() for task in tasks])
    print(f'active concurrency 峰值 peak = {stats['peak']} (limit = {LIMIT})')

asyncio.run(main())
