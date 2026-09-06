import asyncio
from contextlib import asynccontextmanager

from aiohttp import ClientSession, TCPConnector, web

@asynccontextmanager
async def local_server():
    stats = {"active": 0, "peak": 0}
    async def handler(request):
        stats["active"] += 1
        stats["peak"] = max(stats["active"], stats["peak"])
        try:
            await asyncio.sleep(0.1)
            return web.json_response({"path": request.path})
        finally:
            stats["active"] -= 1

    app = web.Application()
    app.router.add_get("/{path:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        yield port, stats
    finally:
        await runner.cleanup()
        print("local server cleanup: 完成")

async def fetch_one(session, url):
    async with asyncio.timeout(1.0):
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
    return url, data

async def main():
    async with local_server() as (port, server_stats):
        urls = [f"http://127.0.0.1:{port}/data/{number}" for number in range(6)]
        connector = TCPConnector(limit = 2)
        async with ClientSession(connector=connector) as session:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(fetch_one(session=session, url=url)) for url in urls]
            results = [task.result() for task in tasks]
        for url, data in results:
            print(url, data)
        print(f"sever 观察到同时处理的 request 峰值 = {server_stats['peak']}"
            f"(connection pool limit=2, 远少于 Task 数量)")

asyncio.run(main())


