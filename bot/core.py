from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from bot.database.db_manager import DatabaseManager
from bot.data_processing.data_updater import update_pair_data

def scheduled_task(pairs):
    """
    Завдання для оновлення даних.
    """
    print(f"🔄 Початок оновлення: {datetime.now()}")
    db_manager = DatabaseManager()
    try:
        for pair in pairs:
            update_pair_data(pair, db_manager)
        print(f"✅ Оновлення завершено: {datetime.now()}")
    except Exception as e:
        print(f"❌ Помилка під час оновлення: {e}")
    finally:
        db_manager.close()

def setup_scheduler(pairs):
    """
    Налаштування розкладу для оновлення даних.
    """
    scheduler = BlockingScheduler()
    # Запуск задачі на 0, 15, 30, 45 хвилинах кожної години
    scheduler.add_job(scheduled_task, 'cron', minute='0,15,30,45', args=[pairs])

    print("📅 Розклад оновлення активовано. Натисніть Ctrl+C для зупинки.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("⏹ Розклад зупинено користувачем.")

# Для тестування
if __name__ == "__main__":
    test_pairs = ["BTC/ETH", "BNB/USDT"]
    setup_scheduler(test_pairs)
