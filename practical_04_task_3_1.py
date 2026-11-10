"""Порівняння ThreadPoolExecutor та ProcessPoolExecutor."""
 
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    as_completed,
)
import time
import math
import hashlib
 
 
def io_bound_task(url: str) -> dict:
    """Імітація I/O-bound задачі (завантаження сторінки)."""
    time.sleep(0.5)
    return {"url": url, "status": 200, "size": len(url) * 42}
 
 
def cpu_bound_task(n: int) -> dict:
    """CPU-bound задача: обчислення хеш-суми великого діапазону."""
    hasher = hashlib.sha256()
    for i in range(n):
        hasher.update(str(i).encode())
    return {"n": n, "hash": hasher.hexdigest()[:16]}
 
 
def risky_task(value: int) -> float:
    """Задача, що може згенерувати виключення."""
    if value == 0:
        raise ZeroDivisionError("Ділення на нуль!")
    if value < 0:
        raise ValueError(f"Від'ємне значення: {value}")
    return 1000.0 / value
 
 
def benchmark_io_bound():
    """Порівняння послідовного та ThreadPoolExecutor для I/O-bound."""
    urls = [f"https://api.example.com/data/{i}" for i in range(20)]
 
    # Послідовне виконання
    start = time.perf_counter()
    seq_results = [io_bound_task(u) for u in urls]
    seq_time = time.perf_counter() - start
    print(f"Послідовно:                     {seq_time:6.2f} сек")
 
    # ThreadPoolExecutor: executor.map зберігає порядок результатів
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        thr_results = list(executor.map(io_bound_task, urls))
    thr_time = time.perf_counter() - start
    print(f"ThreadPoolExecutor (5 потоків): {thr_time:6.2f} сек")
 
    print(f"Прискорення: {seq_time / thr_time:.2f}x")
    print(f"Результати збігаються: {seq_results == thr_results}")
 
 
def benchmark_cpu_bound():
    """Порівняння послідовного та ProcessPoolExecutor для CPU-bound."""
    tasks = [500_000] * 8
 
    # Послідовне виконання
    start = time.perf_counter()
    seq_results = [cpu_bound_task(n) for n in tasks]
    seq_time = time.perf_counter() - start
    print(f"Послідовно:                       {seq_time:6.2f} сек")
 
    # Для порівняння: потоки НЕ прискорюють CPU-bound через GIL
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(cpu_bound_task, tasks))
    thr_time = time.perf_counter() - start
    print(f"ThreadPoolExecutor (4 потоки):    {thr_time:6.2f} сек  (GIL)")
 
    # ProcessPoolExecutor: окремі процеси обходять GIL
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        proc_results = list(executor.map(cpu_bound_task, tasks))
    proc_time = time.perf_counter() - start
    print(f"ProcessPoolExecutor (4 процеси):  {proc_time:6.2f} сек")
 
    print(f"Прискорення процесів відносно послідовного: {seq_time / proc_time:.2f}x")
    print(f"Прискорення потоків відносно послідовного:  {seq_time / thr_time:.2f}x")
    print(f"Результати збігаються: {seq_results == proc_results}")
 
 
def handle_exceptions():
    """Демонстрація обробки виключень у concurrent.futures."""
    values = [10, 5, 0, -3, 8, 2]
 
    ok, failed = 0, 0
    with ThreadPoolExecutor(max_workers=3) as executor:
        # submit повертає Future; словник дозволяє дізнатися вхідне значення
        futures = {executor.submit(risky_task, v): v for v in values}
 
        # as_completed віддає future у порядку завершення, а не запуску
        for future in as_completed(futures):
            value = futures[future]
            try:
                # result() повторно викидає виключення з потоку-воркера
                result = future.result()
                print(f"  value={value:>3}: результат = {result:.2f}")
                ok += 1
            except ZeroDivisionError as e:
                print(f"  value={value:>3}: ZeroDivisionError — {e}")
                failed += 1
            except ValueError as e:
                print(f"  value={value:>3}: ValueError — {e}")
                failed += 1
 
    print(f"\nУспішно: {ok}, з помилкою: {failed}")
    print("Пул продовжив роботу після винятків — інші задачі виконались.")
 
 
def main():
    print("=" * 60)
    print("=== I/O-bound: ThreadPoolExecutor ===")
    print("=" * 60)
    benchmark_io_bound()
 
    print(f"\n{'=' * 60}")
    print("=== CPU-bound: ProcessPoolExecutor ===")
    print("=" * 60)
    benchmark_cpu_bound()
 
    print(f"\n{'=' * 60}")
    print("=== Обробка виключень у пулах ===")
    print("=" * 60)
    handle_exceptions()
 
 
if __name__ == "__main__":
    main()
