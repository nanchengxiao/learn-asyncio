import asyncio

class RecommendationUnavailable(Exception):
    """Recommendations的业务失败。"""

async def fetch_user():
    """获取用户id"""
    print(f"[fetch user] 开始")
    await asyncio.sleep(0.1)
    print(f"[fetch user] 结束")
    return {"id": 7}

async def fetch_orders():
    """获取订单"""
    print(f"[fetch order] 开始")
    await asyncio.sleep(0.2)
    print(f"[fetch order] 结束")
    return [{"id": 101}, {"id": 102}]

async def fetch_recommendations(orders):
    """获取推荐服务"""
    print(f"[recommendations] {len(orders)} 条 orders 已就绪，开始")
    await asyncio.sleep(0.2)
    raise RecommendationUnavailable("推荐服务失败") # 因教学场景，所以这里固定raise一个error

async def fetch_account(user):
    """获取用户账户余额情况"""
    print(f"[account] user 已就绪，开始")
    await asyncio.sleep(0.1)
    print(f"[account] 完成")
    return {"user_id": user["id"], "balance": 100}

async def user_account_branch():
    """业务分支：user → account"""
    user = await fetch_user()
    account = await fetch_account(user)
    return user, account

async def orders_recommendations_banch():
    """业务分支：orders → recommendations"""
    orders = await fetch_orders()
    try:
        recommendations = await fetch_recommendations(orders)
    except RecommendationUnavailable:
        print("[recommendations] optional 失败，执行降级处理")
        recommendations = None
    return orders, recommendations

async def aggregate():
    async with asyncio.TaskGroup() as tg:
        user_branch = tg.create_task(user_account_branch())
        orders_branch = tg.create_task(orders_recommendations_banch())
    user, account = user_branch.result()
    orders, recommendations= orders_branch.result()
    return {
        "user": user,
        "orders": orders,
        "account": account,
        "recommendations": recommendations,
    }

async def main():
    result = await aggregate()
    if result["recommendations"] is None:
        print("degradation：缺少推荐内容，页面仍返回")
    print(result)

asyncio.run(main())




