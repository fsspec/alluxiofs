import multiprocessing
from multiprocessing import Process, JoinableQueue, Queue
from typing import Callable, Any, Optional
import queue  # 用于处理空队列异常
import threading
import time


class AsyncProcessPool:
    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        # 任务队列，使用 JoinableQueue 以便跟踪任务完成
        self.task_queue = JoinableQueue()
        # 结果队列
        self.result_queue = Queue()
        self.workers = []
        self.task_id_counter = 0

        # ---- 改进的结果处理机制 ----
        # 使用字典存储最终结果
        self._results = {}
        # 使用 threading.Event 来通知 get_result 结果已到达
        self._result_events = {}
        # 需要一个线程锁来保护 _results 和 _result_events
        self._result_lock = threading.Lock()

        # 启动工作进程
        self._start_workers()

        # 在主进程中启动一个 *线程* 来专门处理结果
        self._result_handler_thread = threading.Thread(
            target=self._result_handler_loop,
            daemon=True
        )
        self._result_handler_thread.start()

    def _start_workers(self):
        """启动工作进程"""
        for _ in range(self.num_workers):
            worker = Process(
                target=self._worker_loop,
                daemon=True
            )
            worker.start()
            self.workers.append(worker)

    def _worker_loop(self):
        """工作进程的主循环：不断从队列取任务并执行"""
        while True:
            try:
                # 从任务队列获取任务
                task = self.task_queue.get()

                # 【修复】处理终止信号
                if task is None:
                    self.task_queue.task_done()  # 确认任务完成
                    break

                task_id, func, args, kwargs = task
                try:
                    result = func(*args, **kwargs)
                    self.result_queue.put((task_id, result))
                except Exception as e:
                    # 将异常放入结果队列
                    self.result_queue.put((task_id, e))
                finally:
                    # 无论成功与否，都标记任务完成
                    self.task_queue.task_done()

            except (KeyboardInterrupt, SystemExit):
                break
            except EOFError:  # 队列可能已关闭
                break

    def _result_handler_loop(self):
        """
        在主进程中运行的 *线程* 循环。
        专门负责从 result_queue 中取出结果，并存入字典。
        """
        while True:
            try:
                # 阻塞等待结果
                task_id, result = self.result_queue.get()

                with self._result_lock:
                    # 存储结果
                    self._results[task_id] = result
                    # 通知正在等待的 get_result()
                    if task_id in self._result_events:
                        self._result_events[task_id].set()

            except (EOFError, OSError):  # 队列关闭时退出
                break
            except (KeyboardInterrupt, SystemExit):
                break

    def apply_async(
            self,
            func: Callable,
            args: tuple = (),
            kwargs: dict = None,
    ) -> int:
        """异步提交任务，返回 task_id"""
        if kwargs is None:
            kwargs = {}

        with self._result_lock:
            task_id = self.task_id_counter
            self.task_id_counter += 1
            # 【改进】为这个新任务创建一个 Event
            self._result_events[task_id] = threading.Event()

        self.task_queue.put((task_id, func, args, kwargs))
        return task_id

    def get_result(self, task_id: int, timeout: float = None) -> Any:
        """
        【改进】获取任务结果（高效阻塞或超时等待）
        """
        event = None
        with self._result_lock:
            # 检查任务是否已经执行完毕
            if task_id in self._results:
                result = self._results.pop(task_id)
                self._result_events.pop(task_id, None)  # 清理 event
                if isinstance(result, Exception):
                    raise result
                return result

            # 任务未完成，获取 event 以便等待
            event = self._result_events.get(task_id)

        if event is None:
            raise ValueError(f"No such task_id: {task_id} (or result already fetched)")

        # 等待 event 被 _result_handler_loop 设置
        if not event.wait(timeout=timeout):
            raise TimeoutError(f"Timeout waiting for task {task_id}")

        # 任务已完成，获取结果
        with self._result_lock:
            result = self._results.pop(task_id)
            self._result_events.pop(task_id)  # 清理 event

        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        """关闭进程池（等待所有任务完成）"""
        # 1. 等待所有已提交的任务被处理
        self.task_queue.join()

        # 2. 向所有工作进程发送终止信号 (None)
        for _ in self.workers:
            self.task_queue.put(None)

        # 3. 等待所有工作进程退出
        for worker in self.workers:
            worker.join()

        # 4. (可选) 关闭队列，但这在 daemon 进程中不是必须的
        # self.task_queue.close()
        # self.result_queue.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# --- 示例用法 (请确保你的函数和参数是可 pickle 的) ---
def multiply(x, y):
    print(f"Working on: {x} * {y}")
    time.sleep(0.5)
    return x * y


def error_task():
    time.sleep(0.1)
    raise ValueError("This is a test error")


if __name__ == '__main__':

    # 你的代码中 multiprocessing.get_time() 在 Python 3.12 中已移除
    # 这里我们使用 time.monotonic()

    with AsyncProcessPool(num_workers=4) as pool:
        task_ids = []
        for i in range(10):
            task_id = pool.apply_async(multiply, args=(i, 2))
            task_ids.append(task_id)

        error_task_id = pool.apply_async(error_task)
        task_ids.append(error_task_id)

        print(f"Submitted {len(task_ids)} tasks.")

        # 按顺序获取结果
        for tid in task_ids:
            try:
                result = pool.get_result(tid, timeout=5.0)
                print(f"Task {tid} result: {result}")
            except Exception as e:
                print(f"Task {tid} failed with: {e}")

    print("Pool closed.")