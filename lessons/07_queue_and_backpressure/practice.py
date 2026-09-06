import asyncio

WORKERS = 2
SENTINEL = object()

async def source():
    for number in range(1, 7):
        await asyncio.sleep(0.01)
        yield number

async def producer(queue):
    async for item in source():
        print(f'producer 尝试 put {item}')
        await queue.put(item)
        print(f'producer 完成 put {item}(Queue 中 {queue.qsize()} 条)')
    for _ in range(WORKERS):  
        await queue.put(SENTINEL)  # 每个worker 一个结束标记，即：完成前面那个for 循环里的number后，再queue里put接连put两个SENTINEL，worker一拿到这个结束标记就知道要结束了。

async def worker(queue, name):
    while True:
        item = await queue.get()
        try:
            if item is SENTINEL:
                break
            await asyncio.sleep(0.1)
            print(f'[{name}] 完成 {item}')
        finally:
            queue.task_done()

async def main():
    queue = asyncio.Queue(maxsize=2)
    async with asyncio.TaskGroup() as tg:
        for worker_num in range(WORKERS):
            tg.create_task(worker(queue, f'worker_{worker_num}'))
        await producer(queue)
        await queue.join()
    print('pipeline 结束')

asyncio.run(main())


