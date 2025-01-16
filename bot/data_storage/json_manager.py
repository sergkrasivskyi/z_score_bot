import json
from pathlib import Path
from datetime import datetime
import shutil

# Шляхи до файлів
DATA_DIR = Path("bot/data_storage")
MONITORED_PAIRS_FILE = DATA_DIR / "monitoredPairs.json"
FOCUSED_PAIRS_FILE = DATA_DIR / "focusedPairs.json"
BACKUP_DIR = DATA_DIR / "backups"

class JSONManager:
    def __init__(self):
        """Ініціалізація директорій, якщо їх не існує."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    def load_json(self, filepath):
        """Завантаження JSON-даних з файлу."""
        if not filepath.exists():
            return []
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)

    def save_json(self, filepath, data):
        """Збереження JSON-даних до файлу."""
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

    def backup_file(self, filepath):
        """Створення резервної копії файлу."""
        if filepath.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = BACKUP_DIR / f"{filepath.stem}_{timestamp}.json"
            shutil.copy(filepath, backup_path)
            print(f"✅ Резервна копія створена: {backup_path}")
        else:
            print(f"❌ Файл {filepath} не знайдено для резервного копіювання.")

    def get_monitored_pairs(self):
        """Отримання даних про пари з monitoredPairs.json."""
        return self.load_json(MONITORED_PAIRS_FILE)

    def update_monitored_pairs(self, pairs):
        """Оновлення даних у monitoredPairs.json."""
        self.backup_file(MONITORED_PAIRS_FILE)
        self.save_json(MONITORED_PAIRS_FILE, pairs)

    def get_focused_pairs(self):
        """Отримання даних про пари з focusedPairs.json."""
        return self.load_json(FOCUSED_PAIRS_FILE)

    def update_focused_pairs(self, pairs):
        """Оновлення даних у focusedPairs.json."""
        self.backup_file(FOCUSED_PAIRS_FILE)
        self.save_json(FOCUSED_PAIRS_FILE, pairs)

# Для тестування
if __name__ == "__main__":
    manager = JSONManager()

    # Приклад роботи з monitoredPairs.json
    monitored_pairs = manager.get_monitored_pairs()
    print("Monitored Pairs:", monitored_pairs)

    # Оновлення та створення резервної копії
    new_monitored_pairs = [
        {
            "pair": "BTC/ETH",
            "zscore_1m": None,
            "zscore_2m": None,
            "zscore_4m": None,
            "correlation_1m": None,
            "correlation_2m": None,
            "correlation_4m": None,
            "beta_coef_1m": None,
            "beta_coef_2m": None,
            "beta_coef_4m": None,
            "percentile_90_1m": None,
            "percentile_10_1m": None,
            "percentile_90_2m": None,
            "percentile_10_2m": None,
            "percentile_90_4m": None,
            "percentile_10_4m": None,
        }
    ]
    manager.update_monitored_pairs(new_monitored_pairs)

    # Приклад роботи з focusedPairs.json
    focused_pairs = manager.get_focused_pairs()
    print("Focused Pairs:", focused_pairs)
