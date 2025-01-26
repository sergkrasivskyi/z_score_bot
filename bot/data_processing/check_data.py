import sqlite3

DB_PATH = "bot/data_storage/uniq_tokens.db"

def check_data_for_asset(asset_name):
    """
    Перевіряє кількість записів і приклади даних для активу в базі.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Отримання кількості записів
    cursor.execute(
        """
        SELECT COUNT(*) FROM cryptocurrencies
        WHERE name = ?
        """,
        (asset_name,)
    )
    count = cursor.fetchone()[0]

    # Отримання прикладів записів
    cursor.execute(
        """
        SELECT price, timestamp FROM cryptocurrencies
        WHERE name = ?
        ORDER BY timestamp DESC
        LIMIT 5
        """,
        (asset_name,)
    )
    examples = cursor.fetchall()

    conn.close()

    return count, examples

# Тест для PIXELUSDT
asset = "PIXEL"
count, examples = check_data_for_asset(asset)

print(f"Кількість записів для {asset}: {count}")
print("Приклади записів:")
for example in examples:
    print(example)
