import sqlite3
import numpy as np
import logging
from bot.config.config import DATABASE_PATH as DB_PATH

# Налаштування логування
LOG_FILE = "zscore_calculator.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

def calculate_zscore_for_pair(pair):
    """
    Розраховує Z-Score для пари криптовалют.

    Parameters:
        pair (str): Пара криптовалют у форматі 'BASE/QUOTE'.

    Returns:
        float: Z-Score для пари або NaN, якщо сталася помилка.
    """
    base_asset, quote_asset = pair.split("/")
    
    try:
        # Підключення до бази даних
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        logging.info(f"📊 Початок розрахунку Z-Score для пари {pair}")

        # Отримання даних для базового активу
        cursor.execute(
            """
            SELECT price FROM cryptocurrencies
            WHERE name = ?
            ORDER BY timestamp DESC
            LIMIT 672
            """,
            (f"{base_asset}USDT",)
        )
        base_data = [row[0] for row in cursor.fetchall()]

        # Отримання даних для квотованого активу
        cursor.execute(
            """
            SELECT price FROM cryptocurrencies
            WHERE name = ?
            ORDER BY timestamp DESC
            LIMIT 672
            """,
            (f"{quote_asset}USDT",)
        )
        quote_data = [row[0] for row in cursor.fetchall()]

        conn.close()

        # Перевірка наявності достатньої кількості даних
        if len(base_data) < 672 or len(quote_data) < 672:
            raise ValueError(f"Недостатньо даних для пари {pair}")

        # Перевірка на некоректні ціни
        if any(p == 0 or np.isnan(p) for p in base_data + quote_data):
            raise ValueError(f"Некоректні ціни для пари {pair}")

        # Синтетичний курс
        synthetic_prices = [b / q for b, q in zip(base_data, quote_data)]

        # Розрахунок середнього та стандартного відхилення
        mean = np.mean(synthetic_prices)
        std_dev = np.std(synthetic_prices)

        # Перевірка стандартного відхилення
        if std_dev == 0:
            raise ValueError(f"Стандартне відхилення дорівнює 0 для {pair}")

        # Розрахунок Z-Score
        last_price = synthetic_prices[-1]
        zscore = (last_price - mean) / std_dev

        logging.info(f"✅ Z-Score для {pair}: {zscore:.2f}")
        return round(zscore, 2)
    except Exception as e:
        logging.error(f"❌ Помилка для пари {pair}: {e}")
        return float("nan")  # Повертаємо NaN у разі помилки
def calculate_zscores_for_pairs(pairs):
    """
    Розраховує Z-Score для списку пар криптовалют.

    Parameters:
        pairs (list): Список пар криптовалют у форматі 'BASE/QUOTE'.

    Returns:
        dict: Словник із результатами Z-Score для кожної пари.
    """
    results = {}
    for pair in pairs:
        try:
            zscore = calculate_zscore_for_pair(pair)
            if not np.isnan(zscore):
                results[pair] = zscore
                logging.info(f"✅ Z-Score для {pair}: {zscore}")
            else:
                results[pair] = None
                logging.warning(f"⚠️ Не вдалося розрахувати Z-Score для {pair}")
        except Exception as e:
            results[pair] = None
            logging.error(f"❌ Помилка для пари {pair}: {e}")
    return results

if __name__ == "__main__":
    # Тестовий набір пар
    test_pairs = [
        "PIXEL/YGG", "CYBER/LINA", "XAI/LINA", "XAI/YGG", "XAI/BAKE",
        "CYBER/AMB", "PORTAL/YGG", "CYBER/BAKE", "AAVE/ENS", "OGN/LINA",
        "MANA/SLP", "MANA/ROSE", "NEAR/BAND", "FLOW/VANRY", "FLOW/CHZ",
        "FLOW/DODO", "MANTA/GMT", "NEAR/PERP"
    ]

    # Обчислення Z-Score для кожної пари
    zscore_results = calculate_zscores_for_pairs(test_pairs)

    # Вивід результатів
    for pair, zscore in zscore_results.items():
        if zscore is not None:
            print(f"Z-Score для {pair}: {zscore:.2f}")
        else:
            print(f"Не вдалося розрахувати Z-Score для {pair}")


