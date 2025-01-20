# Готова робоча версія, що оновлює таблицю з даними по унікальним активам з таблиці пар bot\data_storage\monitoredPairs.json
import logging
import aiohttp
import asyncio
import time
import json
import sqlite3
from bot.config.config import BINANCE_API_KEY  # Імпорт API ключа
from datetime import datetime, timezone
from asyncio import Semaphore

# Налаштування логування
LOG_FILE = "zscore_bot1.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

BASE_URL = "https://api.binance.com"
SEM_LIMIT = 14  # Максимальна кількість паралельних запитів
semaphore = Semaphore(SEM_LIMIT)

DB_PATH = "bot/data_storage/uniq_tokens.db"
MONITORED_PAIRS_PATH = "bot/data_storage/monitoredPairs.json"

async def fetch_prices(session, symbol):
    """
    Отримує останні 672 періоди для активу з Binance API.
    """
    async with semaphore:
        url = f"{BASE_URL}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "15m",
            "limit": 672,
        }
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

        try:
            async with session.get(url, headers=headers, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                return [
                    {"timestamp": int(candle[0] / 1000), "price": float(candle[4])}
                    for candle in data
                ]
        except Exception as e:
            logging.error(f"❌ Помилка завантаження даних для {symbol}: {e}")
            return []

async def save_to_db(asset, prices):
    """
    Зберігає дані в SQLite базу.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cryptocurrencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                price REAL NOT NULL,
                UNIQUE(name, timestamp)
            )
            """
        )

        # Вставка даних у базу
        cursor.executemany(
            """
            INSERT OR IGNORE INTO cryptocurrencies (name, timestamp, price)
            VALUES (?, ?, ?)
            """,
            [
                (asset, datetime.fromtimestamp(price["timestamp"], tz=timezone.utc).isoformat(), price["price"])
                for price in prices
            ]
        )

        conn.commit()
        conn.close()
        logging.info(f"✅ Дані для {asset} успішно збережено в базу.")
    except Exception as e:
        logging.error(f"❌ Помилка запису в базу для {asset}: {e}")

async def process_assets():
    """
    Основна функція для обробки активів із JSON-файлу та завантаження даних у базу.
    """
    start_time = time.time()

    # Читання JSON-файлу
    try:
        with open(MONITORED_PAIRS_PATH, "r") as file:
            monitored_pairs = json.load(file)
        
        unique_assets = set()
        for pair in monitored_pairs:
            base, quote = pair["pair"].split("/")
            unique_assets.update([base, quote])

        logging.info(f"🔢 Кількість унікальних активів для обробки: {len(unique_assets)}")
    except Exception as e:
        logging.error(f"❌ Помилка читання JSON-файлу: {e}")
        return

    async with aiohttp.ClientSession() as session:
        tasks = []
        for asset in unique_assets:
            symbol = f"{asset}USDT"
            tasks.append(fetch_prices(session, symbol))

        all_prices = await asyncio.gather(*tasks)

        for asset, prices in zip(unique_assets, all_prices):
            if prices:
                await save_to_db(asset, prices)

    elapsed_time = time.time() - start_time
    logging.info(f"✅ Усі активи оброблено. Час виконання: {elapsed_time:.2f} секунд.")

if __name__ == "__main__":
    asyncio.run(process_assets())
