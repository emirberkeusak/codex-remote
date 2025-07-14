import asyncio
import aiohttp
import requests
import ssl
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

# === Telegram Ayarları ===
BOT_TOKEN = "7613430563:AAEqMG-x78220EXm9jOuVcMr4jC5LAOPGfM"
CHAT_ID = -4640610190
THREAD_ID = None
USE_THREAD = False
BASE_SEND_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
BASE_GETUPDATES_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

# === Ayarlar ===
EVAL_INTERVAL = 60
COMMAND_INTERVAL = 3
KLINE_LIMIT = 15
last_update_id = 0
is_bot_active = True
movement_task = None
ALERT_STATUS: Dict[str, Dict[str, int]] = {}
MOVEMENT_CHAIN: Dict[str, Dict[str, float]] = {}

local_tz = ZoneInfo("Europe/Tirane")  # Tirana saat dilimi

# === Sembol Listesi ===
def fetch_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    data = requests.get(url).json()
    return [s['symbol'].lower() for s in data['symbols']
            if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING']

# === Telegram Mesaj Gönder ===
async def send_telegram_message(text):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        if USE_THREAD and THREAD_ID:
            payload["message_thread_id"] = THREAD_ID
        try:
            await session.post(BASE_SEND_URL, data=payload)
        except Exception as e:
            print(f"[Telegram] mesaj gönderilemedi: {e}")

# === Hareket Bildirimi ===
async def send_movement(symbol, pct_change, open_price, close_price, open_time, close_time):
    level = "%3" if abs(pct_change) < 5 else "%5" if abs(pct_change) < 10 else "%10"
    emoji = "🟢" if pct_change > 0 else "🔴"
    direction = "YÜKSELİŞ" if pct_change > 0 else "DÜŞÜŞ"
    pct_str = f"{pct_change:+.2f}%"

    start_dt = datetime.fromtimestamp(open_time / 1000, tz=local_tz).strftime("%d/%m/%Y %H:%M:%S")
    end_dt = datetime.fromtimestamp(close_time / 1000, tz=local_tz).strftime("%d/%m/%Y %H:%M:%S")

    msg = (
        f"{symbol.upper()}  {level}\n\n"
        f"{emoji} {direction} ({pct_str})\n\n"
        f"Fiyat Hareketi Başladı: {start_dt}\n"
        f"Fiyat Hareketi Bitti: {end_dt}\n\n"
        f"Başlangıç Fiyatı: {open_price:.4f}\n"
        f"Güncel Fiyat: {close_price:.4f}"
    )
    await send_telegram_message(msg)

# === Zincir Güncelleme Bildirimi ===
async def send_chain_alert(symbol: str, start_price: float, current_price: float, start_time: int, pct_change: float):
    """Zincir takibi sırasında fiyat her %1 hareket ettiğinde bildirim gönder."""
    emoji = "🟢" if pct_change > 0 else "🔴"
    direction = "YÜKSELİŞ" if pct_change > 0 else "DÜŞÜŞ"
    pct_str = f"{pct_change:+.2f}%"

    start_dt = datetime.fromtimestamp(start_time / 1000, tz=local_tz).strftime("%d/%m/%Y %H:%M:%S")

    msg = (
        f"{symbol.upper()} ZİNCİR\n\n"
        f"{emoji} {direction} ({pct_str})\n\n"
        f"Zincir Başlangıcı: {start_dt}\n"
        f"Başlangıç Fiyatı: {start_price:.4f}\n"
        f"Güncel Fiyat: {current_price:.4f}"
    )
    await send_telegram_message(msg)

# === Kline Hareket Analizi ===
async def evaluate_kline_movement():
    global is_bot_active
    symbols = fetch_symbols()
    print(f"[BAŞLATILDI] Toplam {len(symbols)} sembol izleniyor.")
    await send_telegram_message("✅ Bot başlatıldı")
    while True:
        if is_bot_active:
            for symbol in symbols:
                try:
                    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval=1m&limit={KLINE_LIMIT}"
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE

                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
                        async with session.get(url) as resp:
                            klines = await resp.json()
                            if len(klines) < 2:
                                continue

                            open_price = float(klines[0][1])
                            close_price = float(klines[-1][4])
                            open_time = int(klines[0][0])
                            close_time = int(klines[-1][6])
                            pct = ((close_price - open_price) / open_price) * 100 if open_price != 0 else 0.0

                            print(f"[KLINE] {symbol.upper()} | {open_price:.4f} → {close_price:.4f} | {pct:+.2f}%")

                            status = ALERT_STATUS.get(symbol, {"3": -1, "5": -1, "10+": 0})
                            now_ts = int(datetime.now().timestamp())

                            if abs(pct) >= 3 and (now_ts - status["3"] > EVAL_INTERVAL):
                                await send_movement(symbol, pct, open_price, close_price, open_time, close_time)
                                status["3"] = now_ts
                                if symbol not in MOVEMENT_CHAIN:
                                    MOVEMENT_CHAIN[symbol] = {
                                        "start_price": open_price,
                                        "start_time": close_time,
                                        "last_alert_price": close_price,
                                    }

                            if abs(pct) >= 5 and (now_ts - status["5"] > EVAL_INTERVAL):
                                await send_movement(symbol, pct, open_price, close_price, open_time, close_time)
                                status["5"] = now_ts

                            if abs(pct) >= 10:
                                new_level = int(abs(pct))
                                if new_level > status["10+"]:
                                    await send_movement(symbol, pct, open_price, close_price, open_time, close_time)
                                    status["10+"] = new_level

                            if symbol in MOVEMENT_CHAIN:
                                chain = MOVEMENT_CHAIN[symbol]
                                diff_total = ((close_price - chain["start_price"]) / chain["start_price"]) * 100
                                now_ts = int(datetime.now().timestamp())  # Zaman damgası al
                                if diff_total <= -2:
                                    MOVEMENT_CHAIN.pop(symbol, None)
                                    ALERT_STATUS[symbol] = {"3": -1, "5": -1, "10+": 0}  # reset
                                else:
                                    diff_last = ((close_price - chain["last_alert_price"]) / chain["last_alert_price"]) * 100
                                    last_alert_ts = chain.get("last_alert_ts", 0)
                                    if abs(diff_last) >= 1 and (now_ts - last_alert_ts >= 120):  # 2 dk filtresi
                                        await send_chain_alert(symbol, chain["start_price"], close_price, chain["start_time"], diff_total)
                                        chain["last_alert_price"] = close_price
                                        chain["last_alert_ts"] = now_ts  # Son zincir uyarı zamanı güncelle
                                        MOVEMENT_CHAIN[symbol] = chain

                            if abs(pct) < 3:
                                ALERT_STATUS[symbol] = {"3": -1, "5": -1, "10+": 0}
                            else:
                                ALERT_STATUS[symbol] = status

                except Exception as e:
                    print(f"[HATA] {symbol.upper()} işlem hatası: {e}")
        await asyncio.sleep(EVAL_INTERVAL)

# === /start /stop Komutları ===
async def command_polling():
    global is_bot_active, last_update_id, movement_task
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
                params = {"timeout": 5, "offset": last_update_id + 1}
                async with session.get(BASE_GETUPDATES_URL, params=params) as resp:
                    data = await resp.json()
                    if data["ok"]:
                        for result in data["result"]:
                            last_update_id = result["update_id"]
                            message = result.get("message", {})
                            text = message.get("text", "").strip().lower()
                            chat_id = message.get("chat", {}).get("id", "")
                            if chat_id != CHAT_ID:
                                continue
                            if text == "/start":
                                if movement_task is None or movement_task.done():
                                    is_bot_active = True
                                    movement_task = asyncio.create_task(evaluate_kline_movement())
                                    await send_telegram_message("✅ Bot yeniden başlatıldı")
                                else:
                                    await send_telegram_message("ℹ️ Bot zaten aktif")
                            elif text == "/stop":
                                if movement_task and not movement_task.done():
                                    movement_task.cancel()
                                    await send_telegram_message("⛔ Bot durduruldu")
                                else:
                                    await send_telegram_message("ℹ️ Bot zaten durdurulmuş")
        except Exception as e:
            print("[Komut Polling] Hata:", e)
        await asyncio.sleep(COMMAND_INTERVAL)

# === Ana Fonksiyon ===
async def main():
    global movement_task
    movement_task = asyncio.create_task(evaluate_kline_movement())
    await asyncio.gather(command_polling())

if __name__ == "__main__":
    asyncio.run(main())
