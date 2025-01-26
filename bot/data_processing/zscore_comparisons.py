import sqlite3
import numpy as np
import pandas as pd
from scipy.stats import zscore as scipy_zscore
import logging

# Налаштування логування
LOG_FILE = "zscore_comparisons.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

DB_PATH = "bot/data_storage/uniq_tokens.db"

def fetch_data(base_asset, quote_asset):
    """
    Отримує дані для пари активів із бази.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT price FROM cryptocurrencies
        WHERE name = ?
        ORDER BY timestamp DESC
        LIMIT 672
        """,
        (base_asset,)
    )
    base_data = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT price FROM cryptocurrencies
        WHERE name = ?
        ORDER BY timestamp DESC
        LIMIT 672
        """,
        (quote_asset,)
    )
    quote_data = [row[0] for row in cursor.fetchall()]

    conn.close()

    return base_data, quote_data

def calculate_zscore_numpy(synthetic_prices):
    """
    Розрахунок Z-Score за допомогою NumPy.
    """
    mean = np.mean(synthetic_prices)
    std_dev = np.std(synthetic_prices)
    last_price = synthetic_prices[-1]
    return (last_price - mean) / std_dev

def calculate_zscore_scipy(synthetic_prices):
    """
    Розрахунок Z-Score за допомогою SciPy.
    """
    zscores = scipy_zscore(synthetic_prices)
    return zscores[-1]

def calculate_zscore_direct(synthetic_prices):
    """
    Прямий розрахунок Z-Score вручну.
    """
    mean = sum(synthetic_prices) / len(synthetic_prices)
    variance = sum((x - mean) ** 2 for x in synthetic_prices) / len(synthetic_prices)
    std_dev = variance ** 0.5
    last_price = synthetic_prices[-1]
    return (last_price - mean) / std_dev

def run_zscore_comparisons(pair):
    """
    Виконує розрахунок Z-Score для пари різними способами.
    """
    base_asset, quote_asset = pair.split("/")

    try:
        # Отримання даних
        base_data, quote_data = fetch_data(base_asset, quote_asset)

        if len(base_data) < 672 or len(quote_data) < 672:
            raise ValueError(f"Недостатньо даних для пари {pair}")

        # Перевірка даних
        if any(p == 0 or np.isnan(p) for p in base_data + quote_data):
            raise ValueError(f"Некоректні ціни для {pair}")

        # Синтетичний курс
        synthetic_prices = [b / q for b, q in zip(base_data, quote_data)]

        # Розрахунок Z-Score різними способами
        numpy_zscore = calculate_zscore_numpy(synthetic_prices)
        scipy_zscore_value = calculate_zscore_scipy(synthetic_prices)
        direct_zscore = calculate_zscore_direct(synthetic_prices)

        # Логування результатів
        logging.info(f"Z-Score для {pair} (NumPy): {numpy_zscore:.4f}")
        logging.info(f"Z-Score для {pair} (SciPy): {scipy_zscore_value:.4f}")
        logging.info(f"Z-Score для {pair} (Прямий): {direct_zscore:.4f}")

        return {
            "numpy": numpy_zscore,
            "scipy": scipy_zscore_value,
            "direct": direct_zscore
        }
    except Exception as e:
        logging.error(f"❌ Помилка для пари {pair}: {e}")
        return None

if __name__ == "__main__":
    test_pairs = [
        "PIXEL/YGG", "CYBER/LINA", "XAI/LINA", "XAI/YGG", "XAI/BAKE",
        "CYBER/AMB", "PORTAL/YGG", "CYBER/BAKE", "AAVE/ENS", "OGN/LINA",
        "MANA/SLP", "MANA/ROSE", "NEAR/BAND", "FLOW/VANRY", "FLOW/CHZ",
        "FLOW/DODO", "MANTA/GMT", "NEAR/PERP"
    ]

    # Розрахунок Z-Score для кожної пари
    for pair in test_pairs:
        results = run_zscore_comparisons(pair)

        if results:
            print(f"Z-Score для {pair} (NumPy): {results['numpy']:.4f}")
            print(f"Z-Score для {pair} (SciPy): {results['scipy']:.4f}")
            print(f"Z-Score для {pair} (Прямий): {results['direct']:.4f}")
        else:
            print(f"Не вдалося розрахувати Z-Score для {pair}")
