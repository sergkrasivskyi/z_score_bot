from bot.database.db_manager import DatabaseManager
from bot.data_storage.json_manager import JSONManager
import requests
from datetime import datetime

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
        return [
            {"timestamp": int(candle[0] / 1000), "price": float(candle[4])}
            for candle in data
        ]
    except requests.RequestException as e:
        print(f"Помилка завантаження даних для {symbol}: {e}")
        return None

def get_unique_assets(pairs):
    """
    Визначення унікальних активів із переліку пар.
    """
    assets = set()
    for pair in pairs:
        base, quote = pair.split("/")
        assets.update([base, quote])
    return list(assets)

def update_asset_prices(db_manager, asset):
    """
    Оновлення історичних цін для активу.
    """
    symbol = f"{asset}USDT"
    historical_prices = fetch_historical_prices(symbol)

    if historical_prices:
        for data in historical_prices:
            db_manager.insert_or_update_crypto(
                name=symbol,
                timestamp=datetime.utcfromtimestamp(data["timestamp"]).isoformat(),
                price=data["price"]
            )
        print(f"✅ Дані для {symbol} оновлено.")
    else:
        print(f"❌ Не вдалося оновити дані для {symbol}.")

def update_all_assets():
    """
    Оновлення історичних даних для всіх унікальних активів.
    """
    json_manager = JSONManager()
    db_manager = DatabaseManager()

    # Завантаження пар із JSON
    monitored_pairs = json_manager.get_monitored_pairs()
    pairs = [pair["pair"] for pair in monitored_pairs]

    # Визначення унікальних активів
    unique_assets = get_unique_assets(pairs)

    # Оновлення даних для кожного активу
    for asset in unique_assets:
        update_asset_prices(db_manager, asset)

    db_manager.close()

# Для тестування
if __name__ == "__main__":
    update_all_assets()
