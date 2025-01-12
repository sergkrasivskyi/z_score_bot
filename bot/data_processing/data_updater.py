from bot.database.db_manager import DatabaseManager
from bot.data_processing.z_score_calculator import calculate_z_score
import requests

BASE_URL = "https://api.binance.com"

def fetch_historical_prices(symbol, interval="15m", limit=672):
    """
    Завантаження історичних цін для заданого символу.
    """
    endpoint = f"{BASE_URL}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        prices = [float(candle[4]) for candle in data]
        return prices
    except requests.RequestException as e:
        print(f"Помилка завантаження даних для {symbol}: {e}")
        return None

def update_pair_data(pair, db_manager):
    """
    Оновлення даних для пари та запис у базу даних.
    """
    try:
        base_asset, quote_asset = pair.split("/")
        base_prices = fetch_historical_prices(f"{base_asset}USDT")
        quote_prices = fetch_historical_prices(f"{quote_asset}USDT")
        
        if not base_prices or not quote_prices:
            raise ValueError(f"Не вдалося отримати дані для {pair}")
        
        zscore = calculate_z_score(base_prices, quote_prices)
        cross_rate = base_prices[-1] / quote_prices[-1]

        print(f"Z-Score для {pair}: {zscore}, Cross-Rate: {cross_rate}")

        # Зберігання у базі даних
        db_manager.insert_or_update_pair(
            pair=pair,
            zscore=zscore,
            cross_rate=cross_rate
        )
        return zscore
    except Exception as e:
        print(f"Помилка оновлення даних для {pair}: {e}")
        return None

# Для тестування
if __name__ == "__main__":
    db_manager = DatabaseManager()
    pair = "BTC/ETH"
    zscore = update_pair_data(pair, db_manager)
    db_manager.close()
