import aiohttp
import numpy as np
import asyncio

BASE_URL = "https://api.binance.com/api/v3/klines"

async def fetch_prices(session, symbol, interval="15m", limit=672):
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    async with session.get(BASE_URL, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        return [float(candle[4]) for candle in data]  # Ціни закриття

async def calculate_zscore_for_cross_pair(base_asset, quote_asset):
    async with aiohttp.ClientSession() as session:
        try:
            base_prices = await fetch_prices(session, f"{base_asset}USDT")
            quote_prices = await fetch_prices(session, f"{quote_asset}USDT")

            # Перевірка довжини масивів
            if len(base_prices) < 672 or len(quote_prices) < 672:
                raise ValueError(f"Недостатньо даних для {base_asset}/{quote_asset}")

            # Синтетичний курс
            synthetic_prices = [b / q for b, q in zip(base_prices, quote_prices)]

            # Розрахунок середнього та стандартного відхилення
            mean = np.mean(synthetic_prices)
            std_dev = np.std(synthetic_prices, ddof=0)

            # Перевірка стандартного відхилення
            if std_dev == 0:
                raise ValueError(f"Стандартне відхилення дорівнює 0 для {base_asset}/{quote_asset}")

            # Розрахунок Z-Score
            last_price = synthetic_prices[-1]
            zscore = (last_price - mean) / std_dev

            return {
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "current_price": last_price,
                "mean_price": mean,
                "std_dev": std_dev,
                "zscore": round(zscore, 2)
            }
        except Exception as e:
            print(f"❌ Помилка: {e}")
            return None

async def main():
    result = await calculate_zscore_for_cross_pair("PIXEL", "YGG")
    if result:
        print(f"Результат для {result['base_asset']}/{result['quote_asset']}:")
        print(f"  Поточний кроскурс: {result['current_price']}")
        print(f"  Середній кроскурс: {result['mean_price']}")
        print(f"  Стандартне відхилення: {result['std_dev']}")
        print(f"  Z-Score: {result['zscore']}")

if __name__ == "__main__":
    asyncio.run(main())
