import logging
import aiohttp
import asyncio
import time  # Додайте для вимірювання часу
from bot.database.db_manager import DatabaseManager
from bot.data_storage.json_manager import JSONManager
from bot.config.config import BINANCE_API_KEY  # Імпорт API ключа
from datetime import datetime, timezone
from asyncio import Semaphore

# Налаштування логування
LOG_FILE = "zscore_bot.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

BASE_URL = "https://api.binance.com"

SEM_LIMIT = 10  # Максимальна кількість паралельних запитів
semaphore = Semaphore(SEM_LIMIT)

async def fetch_prices(session, symbol, start_time=None):
    async with semaphore:  # Використання семафора для обмеження запитів
        url = f"{BASE_URL}/api/v3/klines"
        headers = {
            "X-MBX-APIKEY": BINANCE_API_KEY
        }
        params = {
            "symbol": symbol,
            "interval": "15m",
            "limit": 672
        }
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)

        try:
            async with session.get(url, headers=headers, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                logging.info(f"✅ Дані для {symbol} отримані. Періоди: {len(data)}.")
                return [
                    {"timestamp": int(candle[0] / 1000), "price": float(candle[4])}
                    for candle in data
                ]
        except Exception as e:
            logging.error(f"❌ Помилка завантаження даних для {symbol}: {e}")
            return []

async def update_asset(session, db_manager, asset, semaphore):
    """
    Оновлення даних для активу, якщо потрібно.
    """
    async with semaphore:  # Використання Semaphore для обмеження паралельних запитів
        start_time = time.time()
        symbol = f"{asset}USDT"

        # Отримання останнього запису
        latest_record = db_manager.fetch_latest_price(symbol)
        start_time_latest = None if not latest_record else datetime.fromisoformat(latest_record[2])
        if start_time_latest:
            logging.info(f"✅ Останній запис для {symbol}: {start_time_latest}.")

        # Завантаження даних
        historical_prices = await fetch_prices(session, symbol, start_time_latest)
        if not historical_prices:
            return

        # Визначення кількості нових і наявних періодів
        existing_timestamps = set(
            datetime.fromisoformat(record[2]).timestamp() for record in db_manager.fetch_all_crypto_prices(symbol)
        )
        new_data = [
            data for data in historical_prices
            if data["timestamp"] not in existing_timestamps
        ]
        logging.info(f"🔢 Нових періодів для {symbol}: {len(new_data)}.")

        # Видалення старих даних
        if len(existing_timestamps) + len(new_data) > 672:
            db_manager.delete_oldest_crypto_prices(symbol, len(existing_timestamps) + len(new_data) - 672)
            logging.info(f"🗑️ Видалено старих періодів для {symbol}: {len(existing_timestamps) + len(new_data) - 672}.")

        # Вставка нових даних
        db_manager.insert_bulk_crypto_prices(symbol, new_data)

        logging.info(f"✅ Дані для {symbol} оновлено. Усього нових періодів: {len(new_data)}. Час обробки: {time.time() - start_time:.2f} секунд.")

async def update_all_assets(semaphore):
    """
    Асинхронне оновлення всіх активів із паралельною обробкою.
    """
    json_manager = JSONManager()
    db_manager = DatabaseManager()

    monitored_pairs = json_manager.get_monitored_pairs()
    pairs = [pair["pair"] for pair in monitored_pairs]
    logging.info(f"🔢 Загальна кількість пар для аналізу: {len(pairs)}.")

    unique_assets = set()
    for pair in pairs:
        base, quote = pair.split("/")
        unique_assets.update([base, quote])
    logging.info(f"🔢 Загальна кількість унікальних активів: {len(unique_assets)}.")

    # Початок заміру часу
    start_time = time.time()
    logging.info("⏳ Початок оновлення всіх активів...")

    async with aiohttp.ClientSession() as session:
        tasks = [update_asset(session, db_manager, asset, semaphore) for asset in unique_assets]
        await asyncio.gather(*tasks)

    # Закінчення заміру часу
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"✅ Усі активи успішно оновлено. Загальний час обробки: {total_time:.2f} секунд.")

    db_manager.close()
