import aiohttp
import numpy as np
import asyncio

BASE_URL = "https://api.binance.com/api/v3/klines"

async def fetch_candlestick_data(session, symbol, interval="15m", limit=672):
    """
    Завантажує дані про ціни з Binance для заданого активу.

    :param session: Сесія aiohttp для запиту.
    :param symbol: Назва активу, наприклад, BTCUSDT.
    :param interval: Таймфрейм (за замовчуванням 15 хвилин).
    :param limit: Кількість інтервалів (672 для тижневої історії).
    :return: Список цін закриття.
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    async with session.get(BASE_URL, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        return [float(candle[4]) for candle in data]  # Ціни закриття

async def calculate_zscore(symbol):
    """
    Розрахунок Z-Score для заданого активу, отримуючи дані напряму з Binance.

    :param symbol: Назва активу, наприклад, BTCUSDT.
    :return: Результат Z-Score розрахунку.
    """
    async with aiohttp.ClientSession() as session:
        try:
            closing_prices = await fetch_candlestick_data(session, symbol)

            # Перевірка кількості даних
            if len(closing_prices) < 672:
                raise ValueError(f"Недостатньо даних для {symbol}. Отримано {len(closing_prices)} інтервалів.")

            # Розрахунок середнього та стандартного відхилення
            mean_price = np.mean(closing_prices)
            std_dev = np.std(closing_prices, ddof=0)

            # Перевірка стандартного відхилення
            if std_dev == 0:
                raise ValueError(f"Стандартне відхилення дорівнює 0 для {symbol}.")

            # Поточна ціна (остання ціна закриття)
            current_price = closing_prices[-1]

            # Розрахунок Z-Score
            zscore = (current_price - mean_price) / std_dev

            return {
                "symbol": symbol,
                "current_price": current_price,
                "mean_price": mean_price,
                "std_dev": std_dev,
                "zscore": round(zscore, 2),
            }
        except Exception as e:
            print(f"❌ Помилка для {symbol}: {e}")
            return None

async def main():
    """
    Основна функція для розрахунку Z-Score для кількох активів.
    """
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    results = await asyncio.gather(*(calculate_zscore(symbol) for symbol in symbols))

    for result in results:
        if result:
            print(f"Актив: {result['symbol']}")
            print(f"  Поточна ціна: {result['current_price']}")
            print(f"  Середня ціна: {result['mean_price']}")
            print(f"  Стандартне відхилення: {result['std_dev']}")
            print(f"  Z-Score: {result['zscore']}")
            print("---")

if __name__ == "__main__":
    asyncio.run(main())
