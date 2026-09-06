import asyncio
from typing import Any

async def fetch_order() -> list[str]:
    """获取订单"""
    await asyncio.leep(0.1) # 获取订单要花0.1s
    return ['order-1','order-2']

async def fetch_recommendations(orders) -> list[str]:
    """订单推荐生成"""
    await asyncio.sleep(0.50)  # 订单推荐生成要花0.5s
    return [f'rec-for-{order}' for order in orders]

async def pape_data() -> dict[str, Any]:
    """获取页面数据（订单推荐）"""
    # 设置获取订单时的时间预算
    async with asyncio.timeout(0.2):
        orders = await fetch_order()

    try:
        async with asyncio.timeout(0.2):
            # 获取订单推荐
            recommendations = await fetch_recommendations(orders)
    except TimeoutError:
        recommendations = None
    return {'orders': orders, 'recommendations': recommendations}

async def failing_worker(name):
    await asyncio.sleep(0.05)
    raise RuntimeError(f"{name} 失败")

async def collect_errors() -> list[str]:
    """收集errors，两个任务同时失败时，如何把多个错误一起保留下来，而不是只处理第一个。"""
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_worker("A"))
            tg.create_task(failing_worker("B"))
    except* RuntimeError as group:
        # except* 在一组异常里按类型匹配，两个失败都被保留
        errors = sorted(str(error) for error in group.exceptions)
    return errors

async def main():
    print(await pape_data())
    print(await collect_errors())




    