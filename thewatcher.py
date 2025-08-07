import requests
import time
import hmac
import hashlib
import pandas as pd
import os
import logging
from dotenv import load_dotenv
load_dotenv()


# API anahtarları
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_KEY.strip():
    raise EnvironmentError("BINANCE_API_KEY environment variable is missing or empty.")
if not API_SECRET or not API_SECRET.strip():
    raise EnvironmentError("BINANCE_API_SECRET environment variable is missing or empty.")

API_KEY = API_KEY.strip()
API_SECRET = API_SECRET.strip().encode()

# Binance Futures API bilgileri
BASE_URL = "https://fapi.binance.com"
ENDPOINT = "/fapi/v1/userTrades"

# İmzalama fonksiyonu
def get_signature(query_string: str) -> str:
    return hmac.new(API_SECRET, query_string.encode('utf-8'), hashlib.sha256).hexdigest()

# Requests with retry and exponential backoff
def request_with_backoff(url, headers=None, max_retries=5, backoff_factor=1):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            if status == 429 or 500 <= status < 600:
                logging.warning(
                    f"Attempt {attempt} failed with status {status} for {url}"
                )
            else:
                return response
        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt} error for {url}: {e}")

        if attempt < max_retries:
            sleep_time = backoff_factor * (2 ** (attempt - 1))
            time.sleep(sleep_time)

    raise Exception(f"All {max_retries} attempts failed for {url}")

def get_server_time():
    try:
        url = f"{BASE_URL}/fapi/v1/time"
        res = requests.get(url)
        res.raise_for_status()
        return res.json()["serverTime"]
    except Exception as e:
        print(f"❌ Sunucu zamanı alınamadı: {e}")
        return None

# Futures'taki tüm sembolleri getir
def get_futures_symbols():
    try:
        url = f"{BASE_URL}/fapi/v1/exchangeInfo"
        res = request_with_backoff(url)
        res.raise_for_status()
        data = res.json()
        return [s['symbol'] for s in data['symbols']]
    except Exception as e:
        logging.error(f"⚠️ Futures sembolleri alınamadı: {e}")
        return []
    
# Açık pozisyonu olan veya geçmişte işlem yapılmış sembolleri getir
def get_traded_symbols():
    try:
        timestamp = int(time.time() * 1000)
        params = f"timestamp={timestamp}"
        signature = get_signature(params)
        headers = {"X-MBX-APIKEY": API_KEY}
        url = f"{BASE_URL}/fapi/v2/account?{params}&signature={signature}"
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        symbols = []
        for pos in data.get("positions", []):
            position_amt = float(pos.get("positionAmt", 0))
            update_time = pos.get("updateTime", 0)
            if position_amt != 0 or update_time > 0:
                symbols.append(pos["symbol"])
        return symbols
    except Exception as e:
        print(f"⚠️ Pozisyon/işlem sembolleri alınamadı: {e}")
        return []

# Tüm semboller için geçmiş işlemleri getir
def get_all_futures_trades():
    all_trades = []
    symbols = get_traded_symbols()
    if not symbols:
        print("⚠️ İşlem yapılmış sembol bulunamadı.")
        return []

    for symbol in symbols:
        logging.info(f"🔄 {symbol} için işlemler çekiliyor...")
        from_id = 0
        while True:
            try:
                timestamp = get_server_time()
                if timestamp is None:
                    print("⚠️ Sunucu zamanına erişilemedi, yerel zaman kullanılacak.")
                    timestamp = int(time.time() * 1000)
                params = f"symbol={symbol}&timestamp={timestamp}&limit=1000&recvWindow=60000"

                signature = get_signature(params)
                headers = {"X-MBX-APIKEY": API_KEY}
                url = f"{BASE_URL}{ENDPOINT}?{params}&signature={signature}"

                response = request_with_backoff(url, headers=headers)
                if response.status_code == 400 and "Invalid symbol" in response.text:
                    break  # geçersiz sembol
                response.raise_for_status()

                data = response.json()
                if not data:
                    break

                for trade in data:
                    trade["symbol"] = symbol  # sembol ekle

                all_trades.extend(data)

                from_id = data[-1]['id'] + 1
                if len(data) < 1000:
                    break
                time.sleep(0.4)
            except Exception as e:
                logging.error(f"❌ {symbol} için hata oluştu: {e}")
                break

    return all_trades

# Excel'e kaydet
def save_to_excel(trades):
    if not trades:
        print("❗ Hiç işlem bulunamadı.")
        return

    df = pd.DataFrame(trades)
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], unit='ms')
    df = df.sort_values(by='time')

    df.to_excel("binance_futures_trade_history.xlsx", index=False)
    print("✅ Excel dosyası oluşturuldu: binance_futures_trade_history.xlsx")

# Çalıştır
if __name__ == "__main__":
    trades = get_all_futures_trades()
    save_to_excel(trades)
