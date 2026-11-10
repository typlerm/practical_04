"""Семафори та Event для управління конкурентним доступом."""

import threading
import time
import random


class ConnectionPool:
    """Пул з'єднань з обмеженням через Semaphore."""

    def __init__(self, max_connections: int = 3):
        self._semaphore = threading.Semaphore(max_connections)
        self._lock = threading.Lock()
        self.max_connections = max_connections
        self.active_connections = 0
        self.total_served = 0
        self.peak_connections = 0
        self.violations = 0  # скільки разів active перевищив max (має бути 0)

    def acquire_connection(self, client_name: str) -> None:
        """Отримання з'єднання (блокується, якщо пул вичерпано)."""
        print(f"[{client_name}] Запит на з'єднання...")
        # Блокується, поки лічильник семафора не стане > 0
        self._semaphore.acquire()
        # Лічильники статистики змінюються під окремим замком
        with self._lock:
            self.active_connections += 1
            self.peak_connections = max(self.peak_connections, self.active_connections)
            if self.active_connections > self.max_connections:
                self.violations += 1

    def release_connection(self, client_name: str) -> None:
        """Повернення з'єднання до пулу."""
        with self._lock:
            self.active_connections -= 1
            self.total_served += 1
        # Спершу оновлюємо лічильники, потім звільняємо слот
        self._semaphore.release()
        print(f"  [{client_name}] З'єднання повернено")

    def get_stats(self) -> dict:
        """Повертає статистику використання пулу."""
        return {
            "max": self.max_connections,
            "active": self.active_connections,
            "total_served": self.total_served,
            "peak": self.peak_connections,
            "violations": self.violations,
        }


def client_task(pool: ConnectionPool, client_name: str,
                ready_event: threading.Event) -> None:
    """Клієнт, що чекає сигналу готовності та використовує з'єднання."""
    # Усі потоки блокуються тут до виклику ready_event.set()
    ready_event.wait()

    pool.acquire_connection(client_name)
    try:
        work_time = random.uniform(0.5, 2.0)
        print(f"  [{client_name}] Працює {work_time:.1f} сек "
              f"(активних: {pool.active_connections})")
        time.sleep(work_time)
    finally:
        # finally гарантує повернення з'єднання навіть при помилці
        pool.release_connection(client_name)


def main():
    pool = ConnectionPool(max_connections=3)
    ready_event = threading.Event()
    num_clients = 10

    # Створюємо та запускаємо потоки клієнтів
    threads = []
    for i in range(num_clients):
        t = threading.Thread(
            target=client_task,
            args=(pool, f"Client-{i:02d}", ready_event),
        )
        threads.append(t)
        t.start()

    # Даємо всім потокам час стартувати, потім сигналізуємо про готовність
    time.sleep(0.5)
    print(">>> Сервер готовий! Усі клієнти починають роботу.\n")
    ready_event.set()

    # Чекаємо завершення всіх потоків
    for t in threads:
        t.join()

    # Статистика пулу
    stats = pool.get_stats()
    print(f"\n=== Статистика пулу ===")
    print(f"Макс. з'єднань: {stats['max']}")
    print(f"Обслуговано клієнтів: {stats['total_served']}")
    print(f"Пікове навантаження: {stats['peak']}")
    print(f"Активних наприкінці: {stats['active']}")
    print(f"Порушень ліміту: {stats['violations']}")
    print(f"Ліміт ніколи не перевищено: {stats['peak'] <= stats['max']}")


if __name__ == "__main__":
    main()
