from random import randrange
from prefect import flow, task
from prefect.utilities.asyncutils import run_async_from_worker_thread
from prefect_multiprocess import MultiprocessTaskRunner, AnyioMultiprocessTaskRunner
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
import time
import anyio


@task()
def printer(msg):
    delay = 5
    # run_async_from_worker_thread(anyio.sleep, delay)
    anyio.from_thread.run_sync(time.sleep, delay)
    # end = time.time() + delay
    # result = int(msg) * int(msg)
    # while True:
    #     if time.time() > end:
    #         break

    # result = subprocess.run(["sleep", "1"])
    print(f"Message: {msg}")


@flow(task_runner=AnyioMultiprocessTaskRunner(number_of_processes=5))
# @flow(task_runner=ConcurrentTaskRunner())
# @flow(task_runner=SequentialTaskRunner())
def test_flow():
    for i in range(5):
        printer.submit(i)


@task
def task1(x: int) -> int:
    return x + 10


@task
def task2(x: int) -> int:
    return -x


@flow(task_runner=AnyioMultiprocessTaskRunner(number_of_processes=2))
def run_my_flow(n: int):
    task2.map(task1.map(range(n)))


if __name__ == "__main__":
    import time

    start = time.time()
    # print("before MP test flow")
    # test_flow()
    # print("after MP test flow")
    n = 50
    test_flow()
    # print(run_my_flow(n))
    end = time.time()
    mp_time = end - start
    # start = time.time()
    # test_flow_c = test_flow.with_options(task_runner=ConcurrentTaskRunner())()
    # end = time.time()
    print(f"MP time: {mp_time}")
    # print(f"Concurrent elapsed: {end - start}")
