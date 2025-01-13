from bot.core import setup_scheduler

if __name__ == "__main__":
    pairs = ["BTC/ETH", "BNB/USDT"]  # Задайте необхідні пари
    setup_scheduler(pairs)
