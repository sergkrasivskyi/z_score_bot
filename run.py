from asyncio import Semaphore
from bot.core import setup_scheduler
import asyncio
from bot.data_processing.data_updater import update_all_assets

if __name__ == "__main__":
    sem = Semaphore(10)  # Обмеження на 10 паралельних запитів
    print("🔄 Початкове оновлення даних...")
    asyncio.run(update_all_assets(sem))
    print("✅ Початкове оновлення завершено.")

    # Запуск розкладу
    print("📅 Запуск розкладу оновлення...")
    setup_scheduler()

