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
