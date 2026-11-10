"""Producer-Consumer з queue.Queue та демонстрація дедлоку."""

import threading
import queue
import time
import random


class Task:
    """Завдання для обробки."""

    def __init__(self, task_id: int, data: str, priority: int = 0):
        self.task_id = task_id
        self.data = data
        self.priority = priority
        self.created_at = time.time()

    def __repr__(self):
        return f"Task(id={self.task_id}, data='{self.data}')"


def producer(task_queue: queue.Queue, num_tasks: int,
             producer_id: int, state: dict = None) -> None:
    """Виробник: генерує завдання та поміщає їх у чергу.

    Параметр state (необов'язковий) — спільний лічильник активних виробників.
    Sentinel None кладе лише ОСТАННІЙ виробник, інакше споживачі могли б
    завершитись раніше, ніж інший виробник закінчить роботу.
    """
    for i in range(num_tasks):
        task = Task(
            task_id=producer_id * 100 + i,
            data=f"Дані від виробника {producer_id}, пакет {i}",
            priority=random.randint(1, 5),
        )
        # put блокується, якщо черга заповнена (maxsize) — зворотний тиск
        task_queue.put(task)
        print(f"[Виробник-{producer_id}] Створено {task}")
        time.sleep(random.uniform(0.1, 0.3))

    # Сигнал завершення (sentinel)
    if state is None:
        task_queue.put(None)
    else:
        with state["lock"]:
            state["left"] -= 1
            last = state["left"] == 0
        if last:
            task_queue.put(None)
    print(f"[Виробник-{producer_id}] Завершив роботу")


def consumer(task_queue: queue.Queue, consumer_id: int,
             results: list, results_lock: threading.Lock) -> None:
    """Споживач: обробляє завдання з черги."""
    while True:
        try:
            # Тайм-аут запобігає вічному очікуванню на порожній черзі
            task = task_queue.get(timeout=2)
        except queue.Empty:
            print(f"  [Споживач-{consumer_id}] Черга порожня, завершення")
            break

        if task is None:
            # Передати сигнал завершення наступному споживачу
            task_queue.put(None)
            break

        # Імітація обробки
        start = time.perf_counter()
        time.sleep(random.uniform(0.1, 0.4))
        processing_time = time.perf_counter() - start
        print(f"  [Споживач-{consumer_id}] Оброблено {task}")

        # Список результатів спільний — захищаємо замком
        with results_lock:
            results.append((task.task_id, consumer_id, processing_time))

        task_queue.task_done()


def producer_consumer_demo():
    """Демонстрація паттерну Producer-Consumer."""
    task_queue = queue.Queue(maxsize=5)
    results = []
    results_lock = threading.Lock()
    num_producers = 2
    num_consumers = 3
    tasks_per_producer = 5

    state = {"left": num_producers, "lock": threading.Lock()}

    producers = [
        threading.Thread(target=producer,
                         args=(task_queue, tasks_per_producer, pid, state),
                         name=f"Producer-{pid}")
        for pid in range(1, num_producers + 1)
    ]
    consumers = [
        threading.Thread(target=consumer,
                         args=(task_queue, cid, results, results_lock),
                         name=f"Consumer-{cid}")
        for cid in range(1, num_consumers + 1)
    ]

    start = time.perf_counter()
    for t in producers + consumers:
        t.start()
    for t in producers + consumers:
        t.join()
    elapsed = time.perf_counter() - start

    expected = num_producers * tasks_per_producer
    avg_time = sum(r[2] for r in results) / len(results) if results else 0.0
    per_consumer = {}
    for _, cid, _ in results:
        per_consumer[cid] = per_consumer.get(cid, 0) + 1

    print(f"\nОброблено задач: {len(results)} з {expected}")
    print(f"Усі задачі оброблено: {len(results) == expected}")
    print(f"Унікальних task_id: {len({r[0] for r in results})}")
    print(f"Середній час обробки: {avg_time:.3f} сек")
    print(f"Розподіл по споживачах: {dict(sorted(per_consumer.items()))}")
    print(f"Загальний час: {elapsed:.2f} сек")


def deadlock_demo():
    """Демонстрація та виявлення потенційного дедлоку."""
    lock_a = threading.Lock()
    lock_b = threading.Lock()
    deadlock_detected = threading.Event()

    def worker_1():
        """Захоплює lock_a, потім lock_b."""
        lock_a.acquire()
        print("[Worker-1] Захопив Lock A")
        time.sleep(0.1)
        acquired = lock_b.acquire(timeout=2)
        if not acquired:
            print("[Worker-1] ДЕДЛОК: не вдалося захопити Lock B!")
            deadlock_detected.set()
            lock_a.release()
            return
        print("[Worker-1] Захопив Lock B")
        lock_b.release()
        lock_a.release()

    def worker_2():
        """Захоплює lock_b, потім lock_a — зворотний порядок!"""
        lock_b.acquire()
        print("[Worker-2] Захопив Lock B")
        time.sleep(0.1)
        acquired = lock_a.acquire(timeout=2)
        if not acquired:
            print("[Worker-2] ДЕДЛОК: не вдалося захопити Lock A!")
            deadlock_detected.set()
            lock_b.release()
            return
        print("[Worker-2] Захопив Lock A")
        lock_a.release()
        lock_b.release()

    safe_lock_a = threading.Lock()
    safe_lock_b = threading.Lock()
    safe_done = []  # імена воркерів, що успішно завершились

    def safe_worker_1():
        """Безпечна версія: завжди захоплює A → B."""
        with safe_lock_a:
            print("[Safe-1] Захопив Lock A")
            time.sleep(0.1)
            with safe_lock_b:
                print("[Safe-1] Захопив Lock B")
        safe_done.append("Safe-1")

    def safe_worker_2():
        """Безпечна версія: той самий порядок A → B."""
        with safe_lock_a:
            print("[Safe-2] Захопив Lock A")
            time.sleep(0.1)
            with safe_lock_b:
                print("[Safe-2] Захопив Lock B")
        safe_done.append("Safe-2")

    # === Демонстрація дедлоку ===
    print("=== Спроба з потенційним дедлоком ===")
    t1 = threading.Thread(target=worker_1)
    t2 = threading.Thread(target=worker_2)
    start = time.perf_counter()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print(f"Час: {time.perf_counter() - start:.2f} сек")

    if deadlock_detected.is_set():
        print(">>> Дедлок виявлено! Використовуємо timeout для запобігання зависанню.\n")
    else:
        print(">>> Дедлок не виник цього разу (залежить від планувальника).\n")

    # === Безпечна версія ===
    print("=== Безпечна версія (однаковий порядок замків) ===")
    s1 = threading.Thread(target=safe_worker_1)
    s2 = threading.Thread(target=safe_worker_2)
    start = time.perf_counter()
    s1.start()
    s2.start()
    # join з тайм-аутом: якби був дедлок, програма не зависла б
    s1.join(timeout=5)
    s2.join(timeout=5)
    elapsed = time.perf_counter() - start
    no_deadlock = not s1.is_alive() and not s2.is_alive() and len(safe_done) == 2
    print(f"Час: {elapsed:.2f} сек")
    print(f">>> Дедлоку немає, обидва воркери завершились: {no_deadlock}")


def main():
    print("=" * 60)
    print("=== Паттерн Producer-Consumer ===")
    print("=" * 60)
    producer_consumer_demo()

    print(f"\n{'=' * 60}")
    print("=== Демонстрація дедлоку ===")
    print("=" * 60)
    deadlock_demo()


if __name__ == "__main__":
    main()
