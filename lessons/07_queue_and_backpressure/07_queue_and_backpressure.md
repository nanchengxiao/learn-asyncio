# 想跳出循环

# Lesson 07 — 让等待中的工作也有明确上限

## 进入本课前

你已经学过 active concurrency、backlog、Semaphore、worker、downstream 和 rate limit。

本课建议分两遍学习。第一遍只沿“产生数据的一侧 → 中间等待区 → 处理数据的一侧”追踪：等待区满时谁会停住，一条数据真正处理完后怎样登记，最后谁确认所有数据都已完成。第二遍再把 Lesson 00 的逐项读取模型换成允许等待的版本，补齐异步数据源的细节。

## 本课新增术语

这一课把前课的 resource 上限扩展到整条数据流水线。词较多，不需要先背；先按“Queue 两侧是谁”“异步数据怎样逐项进入”“怎样处理完再停止”三组建立位置感。

**第一组：Queue、两侧角色与容量**

- **Queue（队列）**：在多份 async 工作之间临时存放待处理 item 的容器；一边放进去，另一边取出来处理。
- **producer（生产者）**：负责产生待处理 item，并把它们放进 Queue 的代码。
- **consumer（消费者）**：从 Queue 取出 item 并处理的代码；本课的 worker 就是 consumer。
- **upstream（上游）**：更早产生数据或工作的那一侧；本课中 producer 和它读取的数据源都属于 upstream。
- **`maxsize`**：Queue 允许同时存放多少个等待中 item 的容量上限。
- **bounded Queue（有容量上限的队列）**：设置了 `maxsize`，因此等待中的 item 数量不能无限增长的 Queue。
- **backpressure（反压）**：downstream 处理不过来时，让 upstream 也慢下来，而不是继续无限堆积等待工作。
- **`queue.put(item)`**：把一个 item 放进 Queue；bounded Queue 已满时，这一步会等待。
- **`queue.get()`**：从 Queue 取出一个 item；Queue 为空时，这一步会等待。
- **`queue.qsize()`**：返回此刻仍留在 Queue 容器里的 item 数量，不包含已经被 worker 取走并正在处理的 item。
- **`queue.task_done()`**：告诉 Queue“刚才取出的一个 item 已经真正处理完成”。
- **`queue.join()`**：等待所有已经放进 Queue 的 item 都被标记为处理完成。

**第二组：异步逐项读取**

- **AsyncIterator（异步迭代器）**：一次具体的异步逐项读取过程；它保存当前位置，而且请求下一项时允许等待。
- **AsyncIterable（异步可迭代对象）**：一份“可以开始异步逐项读取”的数据；它能提供前面刚定义的 AsyncIterator。
- **async generator function（异步生成器函数）**：同时使用 `async def` 和 `yield` 的函数；它是创建 AsyncIterator 的一种简洁写法。
- **async generator object（异步生成器对象）**：调用 async generator function 后得到的对象；它保存暂停位置，同时属于 AsyncIterator 和 AsyncIterable。
- **`StopAsyncIteration`**：AsyncIterator 已经没有下一项时，用来告诉 `async for`“异步遍历结束”的信号。
- **`async for`**：逐项读取 AsyncIterable 的 Python 语法；每次取得下一项时都允许 async 等待。

**第三组：结束通知与完整流水线**

- **sentinel（结束标记）**：放进 Queue 的一个特殊值，用来告诉 consumer“已经没有新工作了”。
- **`object()`**：创建一个没有业务字段、但身份唯一的普通对象；本例用它制作不会与整数 item 混淆的 sentinel。
- **`is`**：检查两个变量是否指向同一个对象；本例用 `item is SENTINEL` 精确识别那一个结束标记。
- **drain（排空）**：停止接收新的工作，但把已经接收的工作继续处理完。
- **pipeline（流水线）**：把多个处理环节串起来，让数据从上一环节流向下一环节的结构。

## 一个例子串起全部术语

下面让一个 producer 逐项读取 6 条数据，再交给两个较慢的 worker。Queue 最多保存 2 条等待中的 item，所以 worker 跟不上时，压力会通过 `put()` 的等待传回 producer。代码就是本课的 `case.py`：

```python
import asyncio

WORKERS = 2
SENTINEL = object()                        # 结束标记：双方约定的特殊值

async def source():
    for item in range(1, 7):
        await asyncio.sleep(0.01)          # 取下一条本身可能需要等待
        yield item

async def producer(queue):
    # 逐项读取 AsyncIterable；不要先把数据源全部读完再入队
    async for item in source():
        print(f"producer 尝试 put {item}")
        await queue.put(item)              # Queue 已满时这里等待 → backpressure
        print(f"producer 完成 put {item}（Queue 中 {queue.qsize()} 条）")
    for _ in range(WORKERS):
        await queue.put(SENTINEL)          # 每个 worker 一个结束标记

async def worker(queue, name):
    while True:
        item = await queue.get()
        try:
            if item is SENTINEL:           # worker 自己识别并干净退出
                break  					   # 跳出循环前finally 拦路先执行，然后while 循环才真正退出
            await asyncio.sleep(0.1)       # 处理这一条 item
            print(f"[{name}] 完成 {item}")
        finally:
            queue.task_done()              # get() 只表示取走，完成后才标记

async def main():
    queue = asyncio.Queue(maxsize=2)        # 由本条 pipeline 创建并拥有
    async with asyncio.TaskGroup() as tg:
        for worker_number in range(WORKERS):
            tg.create_task(worker(queue, f"worker-{worker_number}"))
        await producer(queue)
        await queue.join()                 # 等待所有已放入 Queue 的 item 都完成对应的`task_done()`
    print("pipeline 结束")

asyncio.run(main())
```

一次运行可能看到下面的交错输出；worker 名称和相邻行顺序可能随 scheduling 略有变化：

```text
producer 尝试 put 1
producer 完成 put 1（Queue 中 1 条）
producer 尝试 put 2
producer 完成 put 2（Queue 中 1 条）
producer 尝试 put 3
producer 完成 put 3（Queue 中 1 条）
producer 尝试 put 4
producer 完成 put 4（Queue 中 2 条）
producer 尝试 put 5
[worker-0] 完成 1
producer 完成 put 5（Queue 中 2 条）
[worker-1] 完成 2
producer 尝试 put 6
producer 完成 put 6（Queue 中 2 条）
[worker-0] 完成 3
[worker-1] 完成 4
[worker-0] 完成 5
[worker-1] 完成 6
pipeline 结束
```

把本课知识点对到代码上：

| 术语或知识点                                | 在这个例子里指什么                                                                                      |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Queue**                             | `main()` 创建的 `queue` 是 producer 与两个 worker 之间临时保存待处理 item 的容器                    |
| **producer**                          | `producer()` 逐项读取 `source()`，再把每个 item 放入 Queue                                          |
| **consumer**                          | 两个`worker()` 从 Queue 取出并处理 item；它们就是本例的 consumer                                      |
| **upstream**                          | `source()` 和读取它的 `producer()` 位于 pipeline 较早的一侧                                         |
| **`maxsize`**                       | `maxsize=2` 表示 Queue 内最多保存 2 个尚未被取走的 item                                               |
| **bounded Queue**                     | 设置容量后的`asyncio.Queue(maxsize=2)`，让 backlog 不能无限留在 Queue 中                              |
| **backpressure**                      | Queue 满时`await queue.put(item)` 暂停，producer 因而不能继续读取下一项                               |
| **`queue.put(item)`**               | 把普通数据或 sentinel 放入 Queue；容量满时会等待                                                        |
| **`queue.get()`**                   | worker 取下一项；Queue 暂时为空时会等待新数据到来                                                       |
| **`queue.qsize()`**                 | 输出中的数字只计算仍在 Queue 里的 item，不包括两个 worker 已经取走并正在处理的 item                     |
| **`queue.task_done()`**             | 每个被`get()` 取走的 item，无论普通数据还是 sentinel，完成处理后都对应调用一次                        |
| **`queue.join()`**                  | 等待所有已放入 Queue 的 item 都完成对应的`task_done()`                                                |
| **AsyncIterable / AsyncIterator**     | `source()` 返回的对象既能开始异步遍历，也保存这次遍历已经走到哪里                                     |
| **async generator function / object** | `source` 是含 `yield` 的 `async def`；调用 `source()` 得到保存暂停位置的 async generator object |
| **`StopAsyncIteration`**            | `source()` 的 `for` 循环结束后由 async generator object 在内部发出，`async for` 据此停止          |
| **`async for`**                     | `async for item in source()` 一项一项推进数据源，每次都允许先在 `asyncio.sleep()` 等待              |
| **sentinel**                          | `SENTINEL = object()` 是 producer 与 worker 约定的结束标记；两个 worker 各收到一个                    |
| **`object()` / `is`**             | `object()` 只创建一次唯一标记；worker 用 `is` 判断取到的是否正是同一个对象，而不是某条普通数据      |
| **drain**                             | producer 停止产生新数据后，`queue.join()` 等待已经接收的工作全部处理完成                              |
| **pipeline**                          | `source → producer → Queue → workers` 构成一条有输入、等待区和处理端的流水线                       |

按时间线读输出：

1. `TaskGroup` 创建两个 worker；Queue 为空时，它们先在 `queue.get()` 等待。
2. `producer()` 用 `async for` 从 `source()` 取得第一条数据；每次都先打印“尝试 put”，等 `put()` 返回后才打印“完成 put”。
3. Worker 取走数据后处理约 0.1 秒，而 producer 大约每 0.01 秒就能产生下一条，因此 Queue 很快达到 `maxsize=2`。
4. `producer 尝试 put 5` 后没有立刻出现“完成”，而是先看到 worker 完成 item 1；这段输出直接证明 producer 停在满 Queue 的 `await queue.put(item)`。
5. Worker 再次 `get()` 后腾出位置，put 5 才完成；在这之前 producer 也没有继续向 `source()` 请求 item 6，backpressure 已传回 upstream。
6. 六条普通数据都放入后，producer 再放入两个 sentinel，保证每个 worker 都能收到结束通知。
7. 每个 worker 在 `finally` 中为取出的 item 调用 `task_done()`；收到 sentinel 时同样标记完成再退出。
8. `queue.join()` 确认 backlog 已经 drain，两个 worker 也结束后，`TaskGroup` 才退出并打印“pipeline 结束”。

## 本节目标

学完本节，你应该能够：

- 使用 bounded Queue 连接 producer 与 consumer；
- 解释 backpressure 为什么需要传回 upstream；
- 区分 active concurrency 上限与 backlog 上限；
- 使用 `task_done()` / `join()` 表达“已处理完成”；
- 使用 sentinel 或明确结束规则让 worker 干净退出；
- 用普通 iteration 模型类比 AsyncIterable、AsyncIterator 与 async generator；
- 解释 drain 与“立刻丢弃剩余工作”的区别。

## 为什么需要学习它

前一课用 `Semaphore` 限制了 active concurrency，但还有一个问题没有解决：

> 如果 producer 很快，等待中的工作会不会一直增加？

假设 producer 每秒产生 10 万条 item，而 consumer 每秒只能处理 1 万条。只要这个速度差长期存在，backlog 就会不断变大。

程序不能靠“多给一点内存”永久解决这个问题。必须让等待区有上限，并在 downstream 跟不上时让 upstream 感受到压力。

## 核心理论

### 1. Queue 把 producer 与 consumer 分开

```text
producer
   │ await queue.put(item)
   ▼
Queue
   │
   ├─ consumer 1
   ├─ consumer 2
   └─ consumer 3
```

Producer 只负责产生 item；consumer 只负责处理 item；Queue 负责在两者之间保存尚未处理的内容。

### 2. `maxsize` 给 backlog 建边界

```python
queue = asyncio.Queue(maxsize=10)
```

这里表示：Queue 最多只允许 10 个 item 在里面等待。

当 Queue 已经满时：

```python
await queue.put(item)
```

会等待，而不是无限继续塞入。

这正是 backpressure：

```text
consumer 处理不过来
        ↓
Queue 变满
        ↓
producer 的 put() 开始等待
        ↓
producer 也被迫慢下来
```

### 3. Semaphore 与 Queue 控制不同边界

```text
Semaphore / worker 数量 → 控制 active concurrency
Queue maxsize           → 控制 backlog
```

两者经常需要一起使用。

只用 Queue，可能仍有太多 consumer 同时访问 downstream；只用 Semaphore，又可能有大量工作在其他地方排队。

### 4. Async iteration 是 Lesson 00 逐项读取模型的异步版本

先把两套词一一对应：

| 普通逐项读取       | 异步逐项读取             |
| ------------------ | ------------------------ |
| iterable           | AsyncIterable            |
| iterator           | AsyncIterator            |
| generator function | async generator function |
| generator object   | async generator object   |
| `for`            | `async for`            |
| `StopIteration`  | `StopAsyncIteration`   |

本课的 `source` 同时使用 `async def` 与 `yield`：

```python
async def source():
    await wait_for_next_item()
    yield item
```

因此 `source` 是 async generator function。调用 `source()` 只创建 async generator object；`async for` 每次请求下一项时才推进它。与普通 generator 相比，关键新增能力是：产生下一项之前可以先 `await`。

`async for` 会在内部反复请求下一项，并在看到 `StopAsyncIteration` 时结束。学习者通常不需要自己抛这个异常，但要知道它与 Lesson 00 的 `StopIteration` 承担对应职责。

### 5. 不要先把数据源全部读完

假设 source 是 AsyncIterable：

```python
async for item in source:
    await queue.put(item)
```

这样 producer 每取得一个 item，就尝试放进 Queue。Queue 满后，`put()` 会等待，于是 producer 暂时不会继续读取 source。

这才能把 backpressure 一直传回真正的数据源。

不要先做类似：

```python
items = [item async for item in source]
```

再慢慢入队。这样等于先把全部内容一次性读入内存，Queue 已经来不及限制 upstream 读取。

### 6. `get()` 只表示“取走”，不表示“处理完成”

Consumer 常见结构：

```python
item = await queue.get()
try:
    await handle(item)
finally:
    queue.task_done()
```

`get()` 把 item 从 Queue 取出来；真正处理完成后，再调用 `task_done()`。

`qsize()` 只观察还留在 Queue 里的数量。一个 item 被 worker `get()` 取走后，即使仍在处理，它也不再计入 `qsize()`；因此 `qsize()` 不是“系统里尚未完成的全部工作数”。

另一边可以：

```python
await queue.join()
```

等待所有已经放进 Queue 的 item 都对应完成一次 `task_done()`。

Queue 内部维护的是一笔“未完成计数”：每次成功 `put()` 加一，每次 `task_done()` 减一。少调用一次，`join()` 可能永远等不到零；多调用一次，Python 会用内置异常 `ValueError` 表示计数已经不可能正确。因此关键不只是“记得调用”，而是每个成功 `get()` 的 item 必须恰好对应一次 `task_done()`，sentinel 也不例外。

### 7. Sentinel 用来告诉 worker“没有新工作了”

如果有固定数量 worker，可以在 producer 结束后放入结束标记：

```text
普通 item
普通 item
SENTINEL
SENTINEL
```

每个 worker 读到 sentinel 后结束自己的循环。

Sentinel 不是 asyncio 特殊对象；它只是双方约定的特殊值。本例先用 `object()` 创建唯一对象，再用 `is` 检查身份，因此任何整数 item 都不会与它冲突。若选择 `None` 等普通业务值作为 sentinel，就必须先保证正常数据里绝不会出现同一个值。

### 8. Drain 表达一种停止承诺

停止 pipeline 时有两种很不同的业务策略：

```text
策略 A：立刻停止，剩余工作可以丢弃
策略 B：停止接收新工作，但已经接收的必须处理完
```

策略 B 就是 drain。

本课只先建立这个词；最后一课会把 drain 放进完整程序的停止流程中。

## 脑内执行模型

```text
producer: put put put [Queue 已满……等待] put
consumer:       get ─ 处理 ─ 完成 ─ get ─ 处理
                         时间 →
```

当 consumer 变慢：

```text
consumer 慢
   ↓
Queue 中等待的 item 数量上升
   ↓
达到 maxsize
   ↓
producer await put()
   ↓
source 暂时不再继续读
```

## 常见误解

- **误区：** Queue 越大，程序一定处理得越快。**更准确：** 更大的 Queue 往往只是允许更多 backlog 在内存里等待。
- **误区：** 有 Semaphore 就不需要 Queue。**更准确：** Semaphore 控制 active concurrency；Queue 还能控制 backlog。
- **误区：** producer 可以先把所有输入读完。**更准确：** 这样 backpressure 无法传回真正的数据源。
- **误区：** `queue.get()` 后就算处理完成。**更准确：** 真正完成后还应调用 `task_done()`。
- **误区：** 多调用几次 `task_done()` 更保险。
  **更准确：** 每个取出的 item 必须恰好对应一次；少一次会让 `join()` 等住，多一次会触发 `ValueError`。
- **误区：** Sentinel 会自动结束所有 worker。**更准确：** 它只是一个约定值，worker 必须自己识别并结束。
- **误区：** drain 就是立即停止。
  **更准确：** drain 的承诺恰恰是把已经接收的工作处理完。

## 本节规则总结

1. Producer 产生 item，consumer 处理 item，Queue 在中间传递。
2. `maxsize` 是 Queue 的 backlog 容量上限。
3. Queue 满时 producer 的 `put()` 等待，就是 backpressure 正在工作。
4. Producer 应逐项读取 AsyncIterable，不要提前把全部内容读入内存。
5. `get()` 表示取走；`task_done()` 表示处理完成；`join()` 等待所有已入队 item 完成。
6. Sentinel 可以表达“没有新工作了”。
7. Drain 表达“停止接收新工作，但处理完已接收工作”。
8. AsyncIterable / AsyncIterator 对应 iterable / iterator；async generator 允许在产生下一项前等待。

## 关键问题

1. Queue 在 producer 与 consumer 之间负责什么？
2. upstream 与 downstream 分别是哪一侧？
3. bounded Queue 的 `maxsize` 限制的是 active concurrency 还是 backlog？
4. backpressure 用白话怎样解释？
5. 为什么 Queue 满时让 `put()` 等待是合理行为？
6. AsyncIterable 与普通 iterable 的关键差别是什么？
7. `async for` 为什么适合逐项读取 AsyncIterable？
8. 为什么不能先把 AsyncIterable 全部读完再入队？
9. `get()`、`task_done()`、`join()` 分别表达什么？
10. sentinel 解决什么问题？
11. 为什么本例用 `object()` 创建 sentinel，并用 `is` 而不是普通数据值判断？
12. drain 与立即丢弃剩余工作有什么区别？
13. pipeline 在本课里是什么意思？
14. AsyncIterable 与 AsyncIterator 的区别，怎样类比 iterable 与 iterator？
15. `source()` 为什么是 async generator function？调用它后立刻执行函数体了吗？
16. `StopAsyncIteration` 在 `async for` 中承担什么职责？

## 场景命题

实现一个 producer → bounded Queue → 固定数量 worker 的 pipeline。

数据源很快时，producer 必须被 Queue 的 backpressure 限制，不能提前把所有 job 读进内存。

要求：

- Queue 在 `main()` 中创建，再明确传给 producer 与 worker；
- `maxsize` 必须很小，并在每次 `put()` 前后打印日志，让等待肉眼可见；
- 使用固定数量 worker，每个 `get()` 都必须恰好对应一次 `task_done()`；
- 为每个 worker 发送一个 sentinel，并确保 sentinel 也完成计数；
- `queue.join()` 返回、所有 worker 结束后才能打印 pipeline 完成；
- 解释为什么 `qsize()` 不包含已经被 worker 取走、但还没处理完的 item。

建议按三个小关卡实现：先让一个 producer 与一个 worker 正常传递数据；再设置很小的 `maxsize`，从日志观察 backpressure；最后增加固定数量 worker、sentinel、`task_done()` 与 `join()`，完成干净停止。

---

完成本课后：继续 [Lesson 08 — 让真实 I/O 也遵守 resource 上限](../08_real_io/08_real_io.md)。
