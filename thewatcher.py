import requests
import time
import hmac
import hashlib
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


# API anahtarları
API_KEY = os.getenv("BINANCE_API_KEY", "AexWengZkPZhwegOqvKkPtjOeRBxokLoRAaYoDLzlJAK3k3dn7wnTXXA2arIbVRY").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "J3u7BuTlOpjHMxzn1Gj5Gd21folNpfk1DWH71Oixlx5yYuRq9ysc29uuLZJLxVBU").strip().encode()

# Binance Futures API bilgileri
BASE_URL = "https://fapi.binance.com"
ENDPOINT = "/fapi/v1/userTrades"

# İmzalama fonksiyonu
def get_signature(query_string: str) -> str:
    return hmac.new(API_SECRET, query_string.encode('utf-8'), hashlib.sha256).hexdigest()

# Futures'taki tüm sembolleri getir
def get_futures_symbols():
    try:
        url = f"{BASE_URL}/fapi/v1/exchangeInfo"
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return [s['symbol'] for s in data['symbols']]
    except Exception as e:
        print(f"⚠️ Futures sembolleri alınamadı: {e}")
        return []

# Tüm semboller için geçmiş işlemleri getir
def get_all_futures_trades():
    all_trades = []
    symbols = get_futures_symbols()

    for symbol in symbols:
        print(f"🔄 {symbol} için işlemler çekiliyor...")
        from_id = None
        while True:
            try:
                timestamp = int(time.time() * 1000)
                params = f"symbol={symbol}&timestamp={timestamp}&limit=1000"
                if from_id:
                    params += f"&fromId={from_id}"

                signature = get_signature(params)
                headers = {"X-MBX-APIKEY": API_KEY}
                url = f"{BASE_URL}{ENDPOINT}?{params}&signature={signature}"

                response = requests.get(url, headers=headers)
                if response.status_code == 400 and "Invalid symbol" in response.text:
                    break  # geçersiz sembol
                response.raise_for_status()

                data = response.json()
                if not data:
                    break

                for trade in data:
                    trade["symbol"] = symbol  # sembol ekle

                all_trades.extend(data)

                if len(data) < 1000:
                    break
                from_id = data[-1]['id'] + 1
                time.sleep(0.4)
            except Exception as e:
                print(f"❌ {symbol} için hata oluştu: {e}")
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
