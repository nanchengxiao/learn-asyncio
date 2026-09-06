# Lesson 10 — 先画清业务依赖，再决定工作怎样开始

## 进入本课前

你已经学过 Task ownership、TaskGroup、timeout、cancellation、required / optional dependency、resource 容量、backpressure、connection pool 和 blocking I/O。

## 本课新增术语

- **six-question model（六问模型）**：编码前固定回答六类设计问题的检查表，用来把业务要求先翻译成执行结构。
- **node（节点）**：把一次业务过程画成方框和箭头时，其中代表一份具体工作的方框，例如“获取 user”。
- **edge（依赖箭头）**：连接两个 node 的箭头；它表示箭头后面的 node 必须先拿到前面 node 的结果才能开始。
- **DAG（Directed Acyclic Graph，有向无环图）**：一张用 node 和 edge 表示“谁依赖谁”的图，而且 dependency 不会绕一圈回到自己。
- **branch（分支）**：从同一个并发起点展开的一条依赖链；本例的 `user → account` 与 `orders → recommendations` 是两条 branch。
- **failure semantics（失败语义）**：某个 node 失败后，业务上应该整体失败、degradation，还是继续处理其他结果。
- **service（服务程序）**：这里指持续对其他代码或 request 提供某种业务能力的程序。
- **aggregator（聚合器）**：从多个来源取得数据，再把它们组合成一个业务结果的那层代码。
- **custom exception（自定义异常）**：用一个专门的异常类型表达某类明确失败；本例的 `RecommendationsUnavailable` 只代表 recommendations 这条 optional dependency 不可用。

## 一个例子串起全部术语

下面的 aggregator 要组合 user、orders、account 和 recommendations 四份数据。代码不是看见四个 I/O 就全部同时开始，而是让每个 node 在自己的前置结果准备好后立刻开始，并提前写清 required 与 optional 的失败规则。代码就是本课的 `case.py`：

```python
import asyncio

class RecommendationsUnavailable(Exception):
    """Recommendations 这条 optional dependency 的已知业务失败。"""

async def fetch_user():
    print("[user] 开始")
    await asyncio.sleep(0.1)
    print("[user] 完成")
    return {"id": 7}

async def fetch_orders():
    print("[orders] 开始")
    await asyncio.sleep(0.15)
    print("[orders] 完成")
    return [{"id": 101}, {"id": 102}]

async def fetch_account(user):
    print("[account] user 已就绪，开始")
    await asyncio.sleep(0.1)
    print("[account] 完成")
    return {"user_id": user["id"], "balance": 100}

async def fetch_recommendations(orders):
    print(f"[recommendations] {len(orders)} 条 orders 已就绪，开始")
    await asyncio.sleep(0.2)
    raise RecommendationsUnavailable("推荐服务失败")

async def user_account_branch():
    """一条依赖链：user → account。"""
    user = await fetch_user()
    account = await fetch_account(user)       # edge：account 依赖 user
    return user, account

async def orders_recommendations_branch():
    """另一条依赖链：orders → recommendations。"""
    orders = await fetch_orders()
    try:
        recommendations = await fetch_recommendations(orders)
    except RecommendationsUnavailable:
        print("[recommendations] optional 失败，执行 degradation")
        recommendations = None                # degradation：允许缺少结果
    return orders, recommendations

async def aggregate():
    async with asyncio.TaskGroup() as tg:
        # 两条独立依赖链同时开始；每条链内部用 await 表达自己的 edge
        user_branch = tg.create_task(user_account_branch())
        orders_branch = tg.create_task(orders_recommendations_branch())
    user, account = user_branch.result()
    orders, recommendations = orders_branch.result()
    return {
        "user": user,                            # required
        "orders": orders,                        # required
        "account": account,                      # required
        "recommendations": recommendations,      # optional
    }

async def main():
    result = await aggregate()
    if result["recommendations"] is None:
        print("degradation：缺少推荐内容，页面仍返回")
    print(result)

asyncio.run(main())
```

真实输出：

```text
[user] 开始
[orders] 开始
[user] 完成
[account] user 已就绪，开始
[orders] 完成
[recommendations] 2 条 orders 已就绪，开始
[account] 完成
[recommendations] optional 失败，执行 degradation
degradation：缺少推荐内容，页面仍返回
{'user': {'id': 7}, 'orders': [{'id': 101}, {'id': 102}], 'account': {'user_id': 7, 'balance': 100}, 'recommendations': None}
```

把本课知识点对到代码上：

| 术语或知识点                                | 在这个例子里指什么                                                                                                                                        |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **six-question model**                | 工作单元是四个业务 node；两条 edge 写明依赖；两条 branch 可同时开始；失败分 required / optional；`aggregate()` 是 owner；真实 resource 上限在本例中省略 |
| **node**                              | `fetch_user()`、`fetch_orders()`、`fetch_account()`、`fetch_recommendations()` 各代表一份具体工作                                                 |
| **edge**                              | `await fetch_account(user)` 与 `await fetch_recommendations(orders)` 同时传入 upstream 结果并表达启动顺序                                             |
| **DAG**                               | 四个 node 和两条单向 edge 组成不会绕回起点的业务图；node 是业务工作，不要求每个 node 都单独创建 Task                                                      |
| **branch**                            | `user_account_branch()` 与 `orders_recommendations_branch()` 分别执行一条依赖链；两条 branch 彼此独立，所以可以同时开始                               |
| **failure semantics**                 | `account` 属于 required，没有降级分支；recommendations 捕获自己的业务失败并返回 `None`                                                                |
| **service**                           | 这个最小例子没有启动长期 server；`aggregate()` 可以作为 service 中一次请求对应的业务操作层                                                              |
| **aggregator**                        | `aggregate()` 并发运行两条独立 branch，最后把四份数据组合成一个字典                                                                                     |
| **custom exception**                  | `RecommendationsUnavailable` 精确命名允许 degradation 的失败；该分支只捕获这个已知类型，不会把无关程序错误伪装成 optional 缺失                          |
| **required dependency**               | user、orders、account 必须存在，任何一项向外失败都会让`TaskGroup` 无法正常完成                                                                          |
| **optional dependency / degradation** | recommendations 失败后返回`None`，页面明确减少内容但仍可成立                                                                                            |
| **Task ownership**                    | 两个 branch Task 都属于`aggregate()` 内的 `TaskGroup`，函数返回前两条依赖链都已经结束                                                                 |

按时间线沿 DAG 读取：

1. `aggregate()` 进入 `TaskGroup`，创建两个 branch Task；每个 Task 负责一条完整依赖链。
2. 两个 branch 分别先执行 `fetch_user()` 与 `fetch_orders()`；输出连续出现 user、orders“开始”，证明第一层没有不必要的先后等待。
3. User 完成后，同一 branch 把结果传给 `fetch_account(user)`；此时 orders 尚未完成，证明两条 branch 彼此独立。
4. Orders 完成后，另一条 branch 才调用 `fetch_recommendations(orders)`；它不需要等待 account。
5. Account 得到 required 结果；recommendations 随后抛出已分类的 optional 失败，并 degradation 为 `None`。
6. 两个 branch Task 都结束后 `TaskGroup` 才退出，`aggregate()` 用 `result()` 读取结果并组合四份页面数据。
7. 最终页面明确报告推荐内容缺失，同时保留三个 required 结果。

## 本节目标

学完本节，你应该能够：

- 使用 six-question model 分析业务；
- 把业务 data dependency 画成 DAG；
- 在编码前决定 required / optional failure semantics；
- 让 Task ownership 与业务边界对齐；
- 根据 DAG 判断每个 node 最早什么时候可以开始。

## 为什么需要学习它

到这一阶段，API 已经不再是最难的部分。

真正复杂的是业务本身：

- 哪些步骤彼此独立？
- 哪些步骤必须等 upstream 结果？
- 哪些失败会让整个 operation 失效？
- 哪些失败可以 degradation？
- 哪些步骤共享同一个稀缺 resource？
- 谁拥有每个 Task？

如果这些问题没有先回答，代码很容易变成“看到 I/O 就 create_task”，最后得到错误的 concurrency 结构。

## 核心理论

### 1. 先回答 six-question model

面对一个业务，编码前先回答：

1. 工作单元是什么？
2. 谁依赖谁？
3. 哪些工作可以同时开始？
4. 稀缺 resource 的 concurrency limit 是什么？
5. 失败、timeout、cancellation 分别怎样影响业务结果？
6. 每个 Task 的 owner 是谁？

这六问不是新的 asyncio API，而是一套先做业务建模的顺序。

### 2. 用 DAG 表示 data dependency

本课的 aggregator 需要四份数据：

- user：required；
- orders：required；
- account：依赖 user，required；
- recommendations：依赖 orders，optional。

DAG：

```text
operation
  ├─ user (required) ─────────→ account (required)
  └─ orders (required) ───────→ recommendations (optional)
```

箭头表达的是：

```text
user ─→ account
```

意思不是“user 和 account 有关系”这么模糊，而是：

> account 在拿到 user 结果前不能开始。

### 3. DAG 决定 node 最早启动时间

第一层：

```text
user
orders
```

二者只共享 operation 输入，彼此没有 data dependency，所以可以同时开始。

第二层：

```text
account         ← 等 user
recommendations ← 等 orders
```

一旦各自前置结果准备好，它们就可以开始；account 不需要等 recommendations，反过来也一样。

所以 DAG 的核心价值是回答：

> 每个 node 最早什么时候具备开始条件？

DAG 的 node 不必机械地一一变成 Task。Task 表示一份需要独立调度和管理 lifecycle 的并发工作；本例真正彼此独立的是两条 branch，因此 `TaskGroup` 只创建两个 Task：

```python
user_branch = tg.create_task(user_account_branch())
orders_branch = tg.create_task(orders_recommendations_branch())
```

同一 branch 内部的 edge 用普通 `await` 和数据传递直接表达：

```python
user = await fetch_user()
account = await fetch_account(user)
```

这样代码结构与业务图一致，又不会为了“一个 node 一个 Task”增加没有调度价值的 Task 引用。

### 4. DAG 不等于 resource 上限

即使 DAG 允许两个 node 同时开始，也不代表它们一定应该无限 concurrency。

例如两个 node 都访问同一个只有少量 connection 的 downstream，那么还要同时应用前面学过的 resource 容量限制。

所以要分开：

```text
DAG           → 业务 dependency 允许什么时候开始
resource 容量 → resource 最多允许多少工作同时占用
```

### 5. Failure semantics 先于 `except`

先决定业务规则，再写异常代码。

例如：

```text
account 失败
→ required
→ 当前完整业务结果不能成立

recommendations 失败
→ optional
→ 可以 degradation
```

不要反过来先写：

```python
except Exception:
    ...
```

然后再临时决定“这个异常好像可以忽略”。

`except` 是实现手段；failure semantics 是业务规则。

本例先把“推荐来源不可用”命名成专用异常：

```python
class RecommendationsUnavailable(Exception):
    pass
```

这行普通 Python 代码声明了一个新的异常类型。Recommendations 分支只捕获这个已知失败并 degradation；拼写错误、状态错误等未知程序缺陷仍会向外暴露。`optional` 决定的是哪些明确失败允许缺少结果，不是给整段代码套一个宽泛的异常过滤器。

### 6. Optional 不代表可以吞掉 cancellation

Recommendations 是 optional，只表示它的业务结果可以缺失。

如果整个 operation 已经收到 cancellation，上层根本不再需要结果，就不应该因为 recommendations optional 而继续吞掉 cancellation。

所以需要继续保持前面建立的规则：

```text
optional 依赖失败 → 可以 degradation
调用者发来 cancellation → 继续 propagation
```

### 7. Task ownership 应与业务边界一致

一次 operation 创建的短 lifecycle Task，通常应该由这次 operation 对应的代码边界负责。

不要让 operation 已经返回，但它创建的业务 Task 还在后台孤立运行。

如果某份工作确实需要超过 operation lifecycle，就必须有一个更长 lifecycle、明确的 owner 接管它。

## 脑内执行模型

```text
第一步：同时开始 user + orders

第二步：user 完成   ─────────→ 开始 account
        orders 完成 ─────────→ 开始 recommendations

第三步：account 是 required
        recommendations 是 optional

第四步：应用 failure semantics
        组合最终结果
```

同时还要叠加 resource 限制：

```text
DAG 允许开始
    ↓
resource 是否还有容量？
    ↓
有 → 真正进入 downstream
无 → 等待 resource
```

## 常见误解

- **误区：** 看见四个 I/O 就全部同时开始。**更准确：** DAG 决定每个 node 最早启动时间。
- **误区：** DAG 只是画图，不影响代码。**更准确：** 代码中的 `await` 路径与数据传递必须反映 edge；node 的业务处理不能越过尚未满足的 dependency。
- **误区：** DAG 中每个 node 都必须创建成一个 Task。
  **更准确：** 只把需要独立调度和统一管理 lifecycle 的并发 branch 创建成 Task；同一依赖链通常直接顺序 `await`。
- **误区：** optional 就是 `except Exception: pass`。**更准确：** optional 只说明某些明确业务失败允许 degradation；不能吞掉未知程序错误或调用者的 cancellation。
- **误区：** failure semantics 就是“异常怎么写”。**更准确：** 它先决定业务结果，再决定用什么异常结构实现。
- **误区：** DAG 已经决定了 concurrency limit。**更准确：** DAG 决定 dependency；resource 容量决定同时能占用多少 resource。
- **误区：** 业务建模会降低 concurrency。
  **更准确：** 它减少错误 concurrency；真正独立的工作仍应尽早重叠等待。

## 本节规则总结

1. 先用 six-question model 理清业务，再写 Task。
2. DAG 用 node 和 edge 表达 data dependency。
3. DAG 决定 node 最早什么时候可以开始。
4. Required / optional 属于 failure semantics。
5. Failure semantics 应先于具体 `except` 写法。
6. Optional 分支只捕获事先分类的失败；未知错误仍应暴露，调用者的 cancellation 仍应传播。
7. Task owner 应对应清楚的业务 lifecycle。
8. DAG 与 resource 容量是两条不同约束。
9. DAG node 不等于 Task；Task 粒度应跟随真正独立的并发 branch。

## 关键问题

1. six-question model 的六个问题是什么？
2. node 与 edge 在 DAG 中分别表示什么？
3. DAG 为什么不能有“绕一圈回到自己”的 dependency？
4. 一个 node 最早什么时候可以开始？
5. failure semantics 与“写哪个 except”有什么区别？
6. recommendations optional 失败时应该怎样处理？
7. account required 失败时为什么通常不能当成完整成功？
8. 如果两个 node 无 dependency，却共用同一个小 connection pool，还要考虑什么？
9. Task ownership 为什么应该跟业务 lifecycle 对齐？
10. service 与 aggregator 在本课里分别是什么意思？
11. 为什么本例捕获 `RecommendationsUnavailable`，而不是捕获所有 `Exception` 或 `RuntimeError`？
12. 为什么本例有四个业务 node，却只创建两个 branch Task？

## 场景命题

实现一个 `Async Service Aggregator`。

这个练习名表示“用 async 方式从多个来源取得数据并组合结果的 service”。

业务关系：

- user：required；
- orders：required；
- account：依赖 user，required；
- recommendations：依赖 orders，optional。

要求：

- user / orders 第一层尽早同时开始；
- account 只能在 user 完成后开始；
- recommendations 只能在 orders 完成后开始；
- optional 依赖失败可以 degradation；
- optional 分支只捕获一个明确的失败类型，未知程序错误不能被伪装成 degradation；
- required 依赖失败继续向外报告；
- 调用者的 cancellation 不能被 optional 处理吞掉。

验收输出必须能直接证明 DAG：user 与 orders 都先开始；account 只能出现在 user 完成之后；recommendations 只能出现在 orders 完成之后。再分别让 required 与 optional 分支失败一次，确认前者向外报告、后者才允许 degradation。

---

完成本课后：继续 [Lesson 11 — 把前面机制组合成长期运行的程序](../11_production_asyncio/11_production_asyncio.md)。
