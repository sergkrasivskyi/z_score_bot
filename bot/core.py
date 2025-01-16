import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import asyncio
from asyncio import Semaphore
from bot.data_processing.data_updater import update_all_assets

# Налаштування логування
LOG_FILE = "zscore_bot.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",  # Додаємо кодування UTF-8
)

def scheduled_task():
    """
    Завдання для оновлення даних із перевіркою актуальності.
    """
    print(f"🔄 Початок оновлення даних.")
    logging.info("🔄 Початок оновлення даних.")
    try:
        sem = Semaphore(10)  # Створюємо новий Semaphore для кожного виклику
        asyncio.run(update_all_assets(sem))
        print(f"✅ Оновлення завершено успішно.")
        logging.info("✅ Оновлення завершено успішно.")
    except Exception as e:
        print(f"❌ Помилка під час оновлення: {e}")
        logging.error(f"❌ Помилка під час оновлення: {e}")

def setup_scheduler():
    """
    Налаштування розкладу для оновлення даних.
    """
    scheduler = BlockingScheduler()
    # Запуск задачі на 0, 15, 30, 45 хвилинах кожної години
    scheduler.add_job(scheduled_task, 'cron', minute='0,15,30,45')

    logging.info("📅 Розклад оновлення активовано.")
    print("📅 Розклад оновлення активовано.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("⏹ Розклад зупинено користувачем.")
        print("⏹ Розклад зупинено користувачем.")
