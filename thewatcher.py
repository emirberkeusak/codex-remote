# main.py

import threading
import time
import logging

from copy_trading import init_mt5, monitor_positions, mt5_executor
from telegram_bot import poll_updates

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

if __name__ == "__main__":
    init_mt5(810005950, "nDGbT_4O", "Taurex-Demo")
    threading.Thread(target=poll_updates,      daemon=True).start()
    threading.Thread(target=monitor_positions, daemon=True).start()
    threading.Thread(target=mt5_executor,      daemon=True).start()
    logging.info("🤖 Bot running…")
    try:
        while True:
            time.sleep(0.2)
    except KeyboardInterrupt:
        logging.info("🛑 Shutting down…")
