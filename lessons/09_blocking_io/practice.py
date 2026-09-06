import asyncio
import time 

THREAD_LIMIT = 2

def legacy_loader(profile_id):
    """旧SDK的普通同步函数"""
    print(f"[loader {profile_id}] worker thread 开始")
    time.sleep(0.3)
    print(f"[loader {profile_id}] worker thread 结束")
    return {"profile": profile_id, "data": "……"}

async def load_profile(profile_id, thread_semaphore):
    """to_thread 包那个普通同步函数"""
    async with thread_semaphore:
        return await asyncio.to_thread(legacy_loader, profile_id=profile_id)

async def hearbeat():
    for _ in range(6):
        print("tick: Event Loop 仍在推进其他 Task")
        await asyncio.sleep(0.1)

async def main():
    thread_semaphore = asyncio.Semaphore(THREAD_LIMIT)
    tasks = []
    async with asyncio.TaskGroup() as tg:
        tg.create_task(hearbeat())
        for profile_id in range(1,4):
            tasks.append(tg.create_task(load_profile(profile_id, thread_semaphore=thread_semaphore)))
    profiles = [task.result() for task in tasks]
    print(profiles)
    print(profiles)
    print(f"loader 线程并发上限 = {THREAD_LIMIT}；blocking 调用期间hearbeat 未被拖住")

asyncio.run(main())