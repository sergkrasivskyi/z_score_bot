import numpy as np

def calculate_z_score(base_prices, quote_prices):
    """
    Розрахунок Z-score для синтетичного курсу пари.
    :param base_prices: Список цін базового активу.
    :param quote_prices: Список цін активу котирування.
    :return: Z-score або None у разі помилки.
    """
    try:
        # Перевірка даних
        if len(base_prices) < 672 or len(quote_prices) < 672:
            raise ValueError("Недостатньо даних для розрахунку.")

        if any(p <= 0 or not isinstance(p, (int, float)) for p in base_prices + quote_prices):
            raise ValueError("Некоректні дані у списках цін.")

        # Розрахунок синтетичного курсу
        synthetic_prices = np.divide(base_prices, quote_prices)

        # Розрахунок середнього та стандартного відхилення
        mean = np.mean(synthetic_prices)
        std_dev = np.std(synthetic_prices)

        if std_dev == 0:
            raise ValueError("Стандартне відхилення дорівнює 0.")

        # Розрахунок Z-score
        last_price = synthetic_prices[-1]
        zscore = (last_price - mean) / std_dev
        return round(zscore, 1)
    except Exception as e:
        print(f"Помилка: {e}")
        return None

# Для тестування
if __name__ == "__main__":
    # Дані для тесту
    base_prices_test = [100 + i * 0.1 for i in range(672)]
    quote_prices_test = [50 + i * 0.05 for i in range(672)]
    
    zscore = calculate_z_score(base_prices_test, quote_prices_test)
    print(f"Z-Score: {zscore}")
