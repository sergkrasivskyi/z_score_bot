import numpy as np
from bot.data_processing.z_score_calculator import calculate_zscores_for_pairs

def test_calculate_zscores():
    """
    Тестує розрахунок Z-Score для заданого списку пар.
    """
    test_pairs = [
        "PIXEL/YGG", "CYBER/LINA", "XAI/LINA", "XAI/YGG", "XAI/BAKE",
        "CYBER/AMB", "PORTAL/YGG", "CYBER/BAKE", "AAVE/ENS", "OGN/LINA",
        "MANA/SLP", "MANA/ROSE", "NEAR/BAND", "FLOW/VANRY", "FLOW/CHZ",
        "FLOW/DODO", "MANTA/GMT", "NEAR/PERP"
    ]

    results = calculate_zscores_for_pairs(test_pairs)

    # Перевірка результатів
    for pair, zscore in results.items():
        if zscore is not None:
            print(f"Z-Score для {pair}: {zscore:.2f}")
        else:
            print(f"Не вдалося розрахувати Z-Score для {pair}")

if __name__ == "__main__":
    test_calculate_zscores()
