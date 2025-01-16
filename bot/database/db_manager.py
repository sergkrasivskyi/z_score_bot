import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Налаштування логування
LOG_FILE = "zscore_bot.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

DATABASE_PATH = Path("z_score_bot.db")

class DatabaseManager:
    def __init__(self, db_path=DATABASE_PATH):
        """Ініціалізація менеджера бази даних."""
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        self._initialize_tables()
        logging.info("✅ База даних успішно ініціалізована.")

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
        logging.info("✅ Таблиці створені або вже існують.")

    def insert_or_update_crypto(self, name, timestamp, price):
        """
        Вставка або оновлення історичних даних про криптовалюту.
        """
        try:
            self.cursor.execute("""
            INSERT INTO cryptocurrencies (name, timestamp, price)
            VALUES (?, ?, ?)
            ON CONFLICT(name, timestamp) DO UPDATE SET
                price = excluded.price
            """, (name, timestamp, price))
            self.connection.commit()
          #  logging.info(f"✅ Дані для {name} оновлено: {timestamp}, {price}.")
        except Exception as e:
            logging.error(f"❌ Помилка при оновленні даних для {name}: {e}.")

    def fetch_all_cryptos(self):
        """Отримання всіх записів з таблиці криптовалют."""
        try:
            self.cursor.execute("SELECT * FROM cryptocurrencies")
            result = self.cursor.fetchall()
            logging.info(f"✅ Усі записи з таблиці криптовалют успішно отримані. Загальна кількість записів: {len(result)}.")
            return result
        except Exception as e:
            logging.error(f"❌ Помилка при отриманні записів: {e}.")
            return []

    def fetch_latest_price(self, name):
        """
        Отримання останнього запису про ціну для конкретного активу.
        """
        try:
            self.cursor.execute("""
            SELECT * FROM cryptocurrencies
            WHERE name = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """, (name,))
            result = self.cursor.fetchone()
            logging.info(f"✅ Останній запис для {name} успішно отримано.")
            return result
        except Exception as e:
            logging.error(f"❌ Помилка при отриманні останнього запису для {name}: {e}.")
            return None

    def is_data_fresh(self, name, latest_timestamp):
        """
        Перевірка, чи є дані актуальними.
        """
        try:
            self.cursor.execute("""
            SELECT 1 FROM cryptocurrencies
            WHERE name = ? AND timestamp = ?
            """, (name, latest_timestamp))
            result = self.cursor.fetchone() is not None
            logging.info(f"✅ Актуальність даних для {name}: {result}.")
            return result
        except Exception as e:
            logging.error(f"❌ Помилка при перевірці актуальності даних для {name}: {e}.")
            return False

    def insert_or_update_pair(self, pair, zscore, cross_rate):
        """
        Вставка або оновлення запису для торгової пари.
        """
        try:
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
            logging.info(f"✅ Дані для пари {pair} оновлено: Z-Score {zscore}, Cross-Rate {cross_rate}.")
        except Exception as e:
            logging.error(f"❌ Помилка при оновленні даних для пари {pair}: {e}.")

    def fetch_all_pairs(self):
        """
        Отримання всіх пар із таблиці pairs.
        """
        try:
            self.cursor.execute("SELECT * FROM pairs")
            result = self.cursor.fetchall()
            logging.info(f"✅ Усі пари успішно отримані. Загальна кількість пар: {len(result)}.")
            return result
        except Exception as e:
            logging.error(f"❌ Помилка при отриманні пар: {e}.")
            return []

    def fetch_unique_assets(self):
        """
        Отримання унікальних активів із таблиці пар.
        """
        try:
            self.cursor.execute("""
            SELECT DISTINCT pair FROM pairs
            """)
            pairs = self.cursor.fetchall()
            unique_assets = set()
            for pair in pairs:
                base, quote = pair[0].split("/")
                unique_assets.update([base, quote])
            logging.info(f"✅ Унікальні активи успішно отримані. Загальна кількість активів: {len(unique_assets)}.")
            return unique_assets
        except Exception as e:
            logging.error(f"❌ Помилка при отриманні унікальних активів: {e}.")
            return set()

    def close(self):
        """Закриття з'єднання з базою даних."""
        try:
            self.connection.close()
            logging.info("⏹ З'єднання з базою даних закрито.")
        except Exception as e:
            logging.error(f"❌ Помилка при закритті з'єднання з базою даних: {e}.")
    def fetch_all_crypto_prices(self, name):
        """
        Отримання всіх записів для конкретного активу з таблиці cryptocurrencies.
        """
        try:
            self.cursor.execute("""
            SELECT * FROM cryptocurrencies
            WHERE name = ?
            ORDER BY timestamp ASC
            """, (name,))
            result = self.cursor.fetchall()
            logging.info(f"✅ Усі записи для {name} успішно отримані. Загальна кількість: {len(result)}.")
            return result
        except Exception as e:
            logging.error(f"❌ Помилка при отриманні записів для {name}: {e}.")
            return []
    def delete_oldest_crypto_prices(self, name, excess_count):
        """
        Видаляє найстаріші записи для активу, щоб залишити тільки останні 672.
        :param name: Назва активу.
        :param excess_count: Кількість записів, які потрібно видалити.
        """
        try:
            self.cursor.execute("""
            DELETE FROM cryptocurrencies
            WHERE id IN (
                SELECT id FROM cryptocurrencies
                WHERE name = ?
                ORDER BY timestamp ASC
                LIMIT ?
            )
            """, (name, excess_count))
            self.connection.commit()
            logging.info(f"🗑️ Видалено {excess_count} старих записів для {name}.")
        except Exception as e:
            logging.error(f"❌ Помилка при видаленні старих записів для {name}: {e}.")
    def insert_bulk_crypto_prices(self, name, prices):
        """
        Пакетна вставка нових записів для активу.
        :param name: Назва активу.
        :param prices: Список нових записів (timestamp, price).
        """
        try:
            self.cursor.executemany("""
            INSERT INTO cryptocurrencies (name, timestamp, price)
            VALUES (?, ?, ?)
            ON CONFLICT(name, timestamp) DO UPDATE SET
                price = excluded.price
            """, [(name, datetime.fromtimestamp(p["timestamp"], tz=timezone.utc).isoformat(), p["price"]) for p in prices])
            self.connection.commit()
            logging.info(f"✅ Пакетна вставка виконана для {name}. Кількість записів: {len(prices)}.")
        except Exception as e:
            logging.error(f"❌ Помилка під час пакетної вставки для {name}: {e}.")


# Для тестування
if __name__ == "__main__":
    db_manager = DatabaseManager()
    pairs = db_manager.fetch_all_pairs()
    logging.info(f"Загальна кількість пар для обробки: {len(pairs)}.")
    unique_assets = db_manager.fetch_unique_assets()
    logging.info(f"Загальна кількість унікальних активів: {len(unique_assets)}.")
    db_manager.insert_or_update_crypto("BTC", "2025-01-12T12:00:00", 43000.0)
    print(db_manager.fetch_all_cryptos())
    print(db_manager.fetch_latest_price("BTC"))
    db_manager.close()
