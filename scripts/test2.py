import asyncio

async def task_func():
    try:
        await asyncio.sleep(100)
    # except Exception:                       # 不写 CancelledError 处理
    #     print("caught Exception (would be a bug if Python 3.7)")
    except asyncio.CancelledError:          # ← 这个其实永远不会被走到（前面更宽泛）
        print("caught CancelledError")
        raise

async def task_func2():
    try:
        await asyncio.sleep(100)
    except Exception:                       # 不写 CancelledError 处理
        print("caught Exception (would be a bug if Python 3.7)")
    except asyncio.CancelledError:
        print("caught CancelledError")
        raise
    # finally:
    #     return

async def task_func3():
    cnt = 0
    # try:
    while True:
        # cnt += 1
        await asyncio.sleep(0.5)
        cnt += 1
    # except asyncio.CancelledError:
    #     print("caught CancelledError: cnt = {}".format(cnt))
    #     raise
    # except Exception:                       # 不写 CancelledError 处理
    #     print("caught Exception (would be a bug if Python 3.7)")
    # finally:
    #     print("finished task3")

async def task_func4():
    try:
        while True:
            await asyncio.sleep(0.5)   # ← await 点 1
            print("step 1")
            await asyncio.sleep(0.5)   # ← await 点 2
            print("step 2")
            await asyncio.sleep(0.5)   # ← await 点 3
            print("step 3")
    except asyncio.CancelledError:
        print("cancelled")
        raise

async def main():
    t = asyncio.create_task(task_func3())
    print("main: before create_task")
    await asyncio.sleep(2)
    print("main: after create_task")
    print("main: before t cancel")
    # try:
    #     t.cancel()
    # except asyncio.CancelledError:
    #     print("main got cancelled2")
    # print("main: after t cancel")
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        print("main got cancelled2")
    print("main: after t cancel")
    # except Exception:                       # 不写 CancelledError 处理
    #     print("caught Exception (would be a bug if Python 3.7)")
    # finally:
    #     print("main got cancelled2")

asyncio.run(main())

# import asyncio
#
#
# async def task_func():
#     try:
#         await asyncio.sleep(10)
#     except asyncio.CancelledError:
#         print("cancelled at last")
#         raise
#
#
# async def main():
#     t = asyncio.create_task(task_func())
#     await asyncio.sleep(0.1)  # 让 task 先跑起来,挂在 sleep 上
#
#     print(f"before cancel: done={t.done()}, cancelled={t.cancelled()}")
#     t.cancel()
#     print(f"right after cancel: done={t.done()}, cancelled={t.cancelled()}")
#     #                               ↑ 注意!这里 done 和 cancelled 都还是 False
#
#     try:
#         print(f"after await t1: done={t.done()}, cancelled={t.cancelled()}")
#         await t
#         print(f"after await t2: done={t.done()}, cancelled={t.cancelled()}")
#     except asyncio.CancelledError:
#         pass
#
#     print(f"after await t3: done={t.done()}, cancelled={t.cancelled()}")
#     #                          ↑ 现在才变成 True
#
#
# asyncio.run(main())