import asyncio
from bot.data_processing.data_672 import process_assets

if __name__ == "__main__":
    print("🔄 Запуск обробки активів...")
    asyncio.run(process_assets())
    print("✅ Обробка активів завершена.")
