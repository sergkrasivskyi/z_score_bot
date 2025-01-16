import sqlite3
from pathlib import Path

DATABASE_PATH = Path("z_score_bot.db")

class DatabaseManager:
    def __init__(self, db_path=DATABASE_PATH):
        """Ініціалізація менеджера бази даних."""
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        self._initialize_tables()

    def _initialize_tables(self):
        """Створення необхідних таблиць, якщо вони не існують."""
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS cryptocurrencies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            price REAL NOT NULL,
            UNIQUE(name, timestamp)
        )
        """)
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            zscore REAL,
            cross_rate REAL,
            max_zscore REAL,
            max_cross_rate REAL,
            dynamic_log TEXT,
            entry_cross_rate REAL,
            UNIQUE(pair)
        )
        """)
        self.connection.commit()

    def insert_or_update_crypto(self, name, timestamp, price):
        """
        Вставка або оновлення історичних даних про криптовалюту.
        """
        self.cursor.execute("""
        INSERT INTO cryptocurrencies (name, timestamp, price)
        VALUES (?, ?, ?)
        ON CONFLICT(name, timestamp) DO UPDATE SET
            price = excluded.price
        """, (name, timestamp, price))
        self.connection.commit()

    def fetch_all_cryptos(self):
        """Отримання всіх записів з таблиці криптовалют."""
        self.cursor.execute("SELECT * FROM cryptocurrencies")
        return self.cursor.fetchall()

    def fetch_latest_price(self, name):
        """
        Отримання останнього запису про ціну для конкретного активу.
        """
        self.cursor.execute("""
        SELECT * FROM cryptocurrencies
        WHERE name = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """, (name,))
        return self.cursor.fetchone()

    def is_data_fresh(self, name, latest_timestamp):
        """
        Перевірка, чи є дані актуальними.
        """
        self.cursor.execute("""
        SELECT 1 FROM cryptocurrencies
        WHERE name = ? AND timestamp = ?
        """, (name, latest_timestamp))
        return self.cursor.fetchone() is not None

    def insert_or_update_pair(self, pair, zscore, cross_rate):
        """
        Вставка або оновлення запису для торгової пари.
        """
        self.cursor.execute("""
        INSERT INTO pairs (pair, zscore, cross_rate, max_zscore, max_cross_rate, dynamic_log, entry_cross_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            zscore = excluded.zscore,
            cross_rate = excluded.cross_rate,
            max_zscore = MAX(pairs.max_zscore, excluded.zscore),
            max_cross_rate = MAX(pairs.max_cross_rate, excluded.cross_rate)
        """, (pair, zscore, cross_rate, zscore, cross_rate, None, cross_rate))
        self.connection.commit()

    def close(self):
        """Закриття з'єднання з базою даних."""
        self.connection.close()

# Для тестування
if __name__ == "__main__":
    db_manager = DatabaseManager()
    db_manager.insert_or_update_crypto("BTC", "2025-01-12T12:00:00", 43000.0)
    print(db_manager.fetch_all_cryptos())
    print(db_manager.fetch_latest_price("BTC"))
    db_manager.close()
