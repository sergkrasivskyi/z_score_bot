import sqlite3
import numpy as np
import logging
import sys
sys.path.append("D:/CryptoBots/Crypto_Way/Trade_bots/zscore_bot_py")
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

def fetch_synchronized_data(cursor, base_asset, quote_asset):
    """
    Отримує синхронізовані по timestamp дані для двох активів.

    Parameters:
        cursor: SQLite курсор для виконання запитів.
        base_asset (str): Назва базового активу.
        quote_asset (str): Назва квотованого активу.

    Returns:
        tuple: Два списки синхронізованих цін для базового і квотованого активів.
    """
    query = """
        SELECT b.price, q.price
        FROM cryptocurrencies b
        JOIN cryptocurrencies q
        ON b.timestamp = q.timestamp
        WHERE b.name = ? AND q.name = ?
        ORDER BY b.timestamp DESC
        LIMIT 672
    """
    cursor.execute(query, (base_asset, quote_asset))
    data = cursor.fetchall()

    base_data = [row[0] for row in data]
    quote_data = [row[1] for row in data]

    return base_data, quote_data

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

        # Отримання синхронізованих даних для активів
        base_data, quote_data = fetch_synchronized_data(cursor, base_asset, quote_asset)

        conn.close()

        # Логування кількості записів
        logging.info(f"Кількість синхронізованих записів для {base_asset}: {len(base_data)}")
        logging.info(f"Кількість синхронізованих записів для {quote_asset}: {len(quote_data)}")

        # Перевірка наявності достатньої кількості даних
        if len(base_data) < 672 or len(quote_data) < 672:
            raise ValueError(f"Недостатньо даних для пари {pair}")

        # Перевірка на некоректні ціни
        if any(p == 0 or np.isnan(p) for p in base_data):
            logging.error(f"Некоректні ціни для {base_asset}: {base_data}")
            raise ValueError(f"Некоректні ціни для {base_asset}")
        if any(p == 0 or np.isnan(p) for p in quote_data):
            logging.error(f"Некоректні ціни для {quote_asset}: {quote_data}")
            raise ValueError(f"Некоректні ціни для {quote_asset}")

        # Синтетичний курс
        synthetic_prices = [b / q for b, q in zip(base_data, quote_data)]

        # Розрахунок середнього та стандартного відхилення
        mean = np.mean(synthetic_prices)
        std_dev = np.std(synthetic_prices, ddof=0)  # Використання популяційного відхилення

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
        return float("nan")

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

    # Узагальнене логування
    success_count = sum(1 for z in results.values() if z is not None)
    logging.info(f"✅ Успішно обчислено Z-Score для {success_count} із {len(pairs)} пар.")

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
