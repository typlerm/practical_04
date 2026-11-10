"""Створення та керування потоками в Python."""

import threading
import time

# Коефіцієнт імітації: скільки секунд "завантажується" 1 МБ
SECONDS_PER_MB = 0.5


def download_simulation(file_name: str, size_mb: float) -> dict:
    """Імітація завантаження файлу (I/O-bound задача).

    Args:
        file_name: назва файлу.
        size_mb: розмір файлу в мегабайтах.

    Returns:
        Словник з інформацією про завантажений файл.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Початок завантаження '{file_name}' ({size_mb} MB)...")
    # Імітація I/O: sleep вивільняє GIL, тому інші потоки працюють паралельно
    time.sleep(size_mb * SECONDS_PER_MB)
    print(f"[{thread_name}] Завершено '{file_name}'")
    return {"file": file_name, "size": size_mb, "thread": thread_name}


class MonitorThread(threading.Thread):
    """Потік-монітор, що періодично виводить стан системи."""

    def __init__(self, interval: float = 1.0):
        # daemon=True: потік не блокує завершення програми
        super().__init__(name="Monitor", daemon=True)
        self.interval = interval
        self.active_downloads = 0
        self._stop_event = threading.Event()

    def run(self):
        """Виводить кількість активних потоків кожні `interval` секунд."""
        while not self._stop_event.is_set():
            # active_count() враховує й сам монітор та головний потік
            print(f"[{self.name}] Активних потоків: {threading.active_count()}")
            # wait() замість sleep(): прокидається одразу після stop()
            self._stop_event.wait(self.interval)

    def stop(self):
        self._stop_event.set()


def main():
    files = [
        ("data.csv", 2.5),
        ("report.pdf", 1.0),
        ("image.png", 0.5),
        ("archive.zip", 4.0),
        ("log.txt", 0.3),
    ]

    # Запускаємо демон-потік моніторингу
    monitor = MonitorThread(interval=1.0)
    monitor.start()

    # Результати збираємо у спільний список (list.append потокобезпечний у CPython)
    results = []

    def worker(name, size):
        results.append(download_simulation(name, size))

    # Створюємо по одному потоку на файл
    threads = [
        threading.Thread(target=worker, args=(name, size), name=f"Download-{i}")
        for i, (name, size) in enumerate(files, start=1)
    ]

    print("=== Паралельне завантаження (threading) ===")
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()  # чекаємо завершення всіх потоків
    elapsed = time.perf_counter() - start

    print(f"\nУсі файли завантажено за {elapsed:.2f} сек")
    print(f"Завантажено файлів: {len(results)}")

    # Порівняння з послідовним виконанням
    print("\n=== Послідовне завантаження ===")
    start = time.perf_counter()
    for name, size in files:
        download_simulation(name, size)
    seq_elapsed = time.perf_counter() - start

    print(f"\nПослідовно: {seq_elapsed:.2f} сек")
    print(f"Паралельно: {elapsed:.2f} сек")
    print(f"Прискорення: {seq_elapsed / elapsed:.2f}x")

    # Зупиняємо монітор через Event
    monitor.stop()
    monitor.join(timeout=2)
    print(f"Монітор зупинено: {not monitor.is_alive()}")


if __name__ == "__main__":
    main()