"""Демонстрація race condition та захист через Lock."""

import threading
import time
import random


class BankAccount:
    """Банківський рахунок без синхронізації (небезпечний)."""

    def __init__(self, balance: float = 0.0):
        self.balance = balance
        self.transaction_count = 0

    def deposit(self, amount: float) -> None:
        """Поповнення рахунку."""
        # Імітація неатомарної операції: читання -> пауза -> запис
        current = self.balance
        time.sleep(random.uniform(0.0001, 0.001))
        self.balance = current + amount
        self.transaction_count += 1

    def withdraw(self, amount: float) -> bool:
        """Зняття коштів. Повертає True, якщо операція успішна."""
        current = self.balance
        time.sleep(random.uniform(0.0001, 0.001))
        if current >= amount:
            self.balance = current - amount
            self.transaction_count += 1
            return True
        return False


class SafeBankAccount:
    """Банківський рахунок із синхронізацією через Lock."""

    def __init__(self, balance: float = 0.0):
        self.balance = balance
        self.transaction_count = 0
        self._lock = threading.Lock()

    def deposit(self, amount: float) -> None:
        # Весь цикл "прочитати-змінити-записати" виконується під замком,
        # тому інші потоки не можуть втрутитися між читанням і записом
        with self._lock:
            current = self.balance
            time.sleep(random.uniform(0.0001, 0.001))
            self.balance = current + amount
            self.transaction_count += 1

    def withdraw(self, amount: float) -> bool:
        with self._lock:
            current = self.balance
            time.sleep(random.uniform(0.0001, 0.001))
            if current >= amount:
                self.balance = current - amount
                self.transaction_count += 1
                return True
            return False


def run_transactions(account, num_deposits: int, num_withdrawals: int,
                     deposit_amount: float = 50.0, withdraw_amount: float = 50.0,
                     num_threads: int = 10) -> int:
    """Запускає потоки для виконання транзакцій.

    Загальна кількість операцій ділиться порівну між num_threads потоками
    поповнення та num_threads потоками зняття (разом 2*num_threads потоків).

    Returns:
        Кількість невдалих зняттів (через брак коштів).
    """
    failed = []  # list.append потокобезпечний у CPython

    def deposit_worker(n):
        for _ in range(n):
            account.deposit(deposit_amount)

    def withdraw_worker(n):
        for _ in range(n):
            if not account.withdraw(withdraw_amount):
                failed.append(1)

    threads = []
    for i in range(num_threads):
        # Розподіляємо залишок, якщо кількість операцій не ділиться націло
        dep_n = num_deposits // num_threads + (1 if i < num_deposits % num_threads else 0)
        wd_n = num_withdrawals // num_threads + (1 if i < num_withdrawals % num_threads else 0)
        threads.append(threading.Thread(target=deposit_worker, args=(dep_n,),
                                        name=f"Deposit-{i}"))
        threads.append(threading.Thread(target=withdraw_worker, args=(wd_n,),
                                        name=f"Withdraw-{i}"))

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return len(failed)


def report(account, expected_balance, total_ops, failed):
    """Виводить порівняння очікуваного та фактичного результату."""
    expected_tx = total_ops - failed
    ok_balance = abs(account.balance - expected_balance) < 1e-9
    ok_tx = account.transaction_count == expected_tx
    print(f"Очікуваний баланс:  {expected_balance:.2f}")
    print(f"Фактичний баланс:   {account.balance:.2f}  -> "
          f"{'збігається' if ok_balance else 'НЕ збігається (race condition!)'}")
    print(f"Транзакцій виконано: {account.transaction_count} (очікувалось {expected_tx})  -> "
          f"{'коректно' if ok_tx else 'НЕКОРЕКТНО'}")
    print(f"Невдалих зняттів:   {failed}")
    return ok_balance and ok_tx


def main():
    initial_balance = 1000.0
    num_deposits = 100
    deposit_amount = 50.0
    num_withdrawals = 100
    withdraw_amount = 50.0

    expected_balance = (
        initial_balance
        + num_deposits * deposit_amount
        - num_withdrawals * withdraw_amount
    )
    total_ops = num_deposits + num_withdrawals

    # === Тест 1: Без синхронізації ===
    print("=== Тест без синхронізації ===")
    unsafe_account = BankAccount(initial_balance)
    start = time.perf_counter()
    failed = run_transactions(unsafe_account, num_deposits, num_withdrawals,
                              deposit_amount, withdraw_amount)
    print(f"Час: {time.perf_counter() - start:.3f} сек, потоків: 20")
    unsafe_ok = report(unsafe_account, expected_balance, total_ops, failed)

    # === Тест 2: З синхронізацією (Lock) ===
    print("\n=== Тест із синхронізацією (Lock) ===")
    safe_account = SafeBankAccount(initial_balance)
    start = time.perf_counter()
    failed = run_transactions(safe_account, num_deposits, num_withdrawals,
                              deposit_amount, withdraw_amount)
    print(f"Час: {time.perf_counter() - start:.3f} сек, потоків: 20")
    safe_ok = report(safe_account, expected_balance, total_ops, failed)

    print(f"\nОчікуваний баланс: {expected_balance:.2f}")
    print(f"Без синхронізації коректно: {unsafe_ok}; з Lock коректно: {safe_ok}")


if __name__ == "__main__":
    main()
