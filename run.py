import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.data_processing.data_672 import process_assets
import logging

# Налаштування логування
LOG_FILE = "zscore_bot_run.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

async def main():
    """
    Головна функція для запуску задач: разовий запуск і розклад.
    """
    scheduler = AsyncIOScheduler()

    # Запуск розкладу
    scheduler.add_job(process_assets, "cron", minute="0,15,30,45")
    scheduler.start()

    # Лог початку роботи
    logging.info("📅 Розклад завдань запущено: кожні 15 хвилин.")

    # Одноразовий запуск перед розкладом
    print("🔄 Виконання першого завдання...")
    logging.info("🔄 Виконання першого завдання...")
    try:
        await process_assets()
        logging.info("✅ Перше завдання виконано успішно.")
        print("✅ Перше завдання завершено.")
    except Exception as e:
        logging.error(f"❌ Помилка під час виконання першого завдання: {e}")
        print(f"❌ Помилка: {e}")

    print("📅 Очікування наступних завдань за розкладом...")
    try:
        await asyncio.Event().wait()  # Безкінечне очікування
    except (KeyboardInterrupt, SystemExit):
        logging.info("❌ Програма зупинена вручну.")
        print("❌ Зупинка програми.")
        scheduler.shutdown()

if __name__ == "__main__":
    print("🔄 Запуск програми...")
    logging.info("🔄 Запуск програми...")
    asyncio.run(main())
