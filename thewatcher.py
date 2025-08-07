import os
from binance import Client
import pandas as pd
from datetime import datetime
import time

# API anahtarlarınız
BINANCE_API_KEY = "AexWengZkPZhwegOqvKkPtjOeRBxokLoRAaYoDLzlJAK3k3dn7wnTXXA2arIbVRY"
BINANCE_API_SECRET = "J3u7BuTlOpjHMxzn1Gj5Gd21folNpfk1DWH71Oixlx5yYuRq9ysc29uuLZJLxVBU"

def get_all_futures_income():
    # Binance istemcisini başlat
    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    
    # API bağlantı testi
    try:
        account_info = client.futures_account()
        print(f"API Bağlantısı Başarılı! Hesap Bakiyesi: {account_info['totalWalletBalance']} USDT")
    except Exception as e:
        print(f"API Bağlantı Hatası: {e}")
        return []
    
    all_income = []
    start_time = None
    fetch_count = 0
    
    print("İşlem geçmişi alınıyor...")
    while True:
        try:
            # Parametreleri hazırla
            params = {'limit': 1000}
            if start_time:
                params['startTime'] = start_time
            
            # Gelir geçmişini çek
            income = client.futures_income_history(**params)
            fetch_count += 1
            
            if not income:
                print("Daha fazla veri bulunamadı.")
                break
                
            all_income.extend(income)
            print(f"Alınan batch: {fetch_count}, Kayıt sayısı: {len(income)}, Toplam: {len(all_income)}")
            
            # Son kaydın zamanını al (bir sonraki sayfa için)
            last_record_time = int(income[-1]['time'])
            start_time = last_record_time + 1
            
            # Rate limit koruması
            time.sleep(0.2)
            
            # Son sayfa kontrolü
            if len(income) < 1000:
                print(f"Tüm veriler alındı. Toplam kayıt: {len(all_income)}")
                break
                
        except Exception as e:
            print(f"Hata oluştu: {e}")
            break

    return all_income

def save_to_excel(income_data):
    if not income_data:
        print("Kaydedilecek işlem bulunamadı.")
        return
    
    # Veriyi DataFrame'e dönüştür
    df = pd.DataFrame(income_data)
    
    # Zaman damgasını düzenle
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    
    # Sütunları yeniden düzenle
    columns = ['time', 'symbol', 'incomeType', 'income', 'asset', 'info', 'tradeId', 'tranId']
    
    # Excel'e kaydet
    filename = f"binance_futures_income_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df[columns].to_excel(filename, index=False)
    print(f"{len(df)} gelir kaydı Excel'e kaydedildi: {filename}")
    return filename

if __name__ == "__main__":
    print("Binance Futures Gelir Geçmişi Aktarımı Başlatılıyor...")
    print(f"Başlangıç Zamanı: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    income_data = get_all_futures_income()
    
    if income_data:
        print(f"\nToplam {len(income_data)} gelir kaydı bulundu")
        print(f"İlk kayıt tarihi: {pd.to_datetime(income_data[0]['time'], unit='ms')}")
        print(f"Son kayıt tarihi: {pd.to_datetime(income_data[-1]['time'], unit='ms')}")
        file_path = save_to_excel(income_data)
        print(f"Excel dosyası oluşturuldu: {file_path}")
    else:
        print("\nHiç gelir kaydı bulunamadı. Olası nedenler:")
        print("1. Bu hesapta hiç işlem yapılmamış olabilir")
        print("2. API anahtarında futures izni eksik olabilir")
        print("3. IP adresiniz Binance API'de kısıtlı olabilir")
    
    print("\nİşlem tamamlandı")