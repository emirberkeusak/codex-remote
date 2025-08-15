# ================================================================
# MONOLITHIC BUILD: copy_trading.py + mt5_diagnostic.py + statistics.py
#                    + telegram_bot.py + main.py  (tek dosya)
# NOT: Çalışma prensibi korunmuştur. Modüllerarası importlar için
#      sys.modules alias yöntemi kullanılmıştır.
# ================================================================

# ---------------------------
# mt5_diagnostic.py (başlangıç)
# ---------------------------
"""
MT5 Data Validation and Diagnostic Tool
Trading logic'e dokunmadan sadece veri doğrulama yapar
"""

import sys
import time
import logging
from datetime import datetime
import MetaTrader5 as mt5
import pandas as pd

# Detaylı loglama için
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
md_logger = logging.getLogger(__name__)

class MT5Diagnostic:
    def __init__(self):
        self.last_positions = {}
        self.position_history = []
        
    def get_retcode_description(self, retcode):
        """MT5 return code açıklaması"""
        retcodes = {
            mt5.TRADE_RETCODE_DONE: "Request completed",
            mt5.TRADE_RETCODE_PLACED: "Order placed",
            mt5.TRADE_RETCODE_MARKET_CLOSED: "Market is closed",
            mt5.TRADE_RETCODE_INVALID: "Invalid request",
            mt5.TRADE_RETCODE_REJECTED: "Request rejected",
            mt5.TRADE_RETCODE_NO_MONEY: "Not enough money",
            mt5.TRADE_RETCODE_INVALID_PRICE: "Invalid price",
            mt5.TRADE_RETCODE_INVALID_VOLUME: "Invalid volume",
            mt5.TRADE_RETCODE_CONNECTION: "No connection",
            mt5.TRADE_RETCODE_TRADE_DISABLED: "Trade is disabled",
            mt5.TRADE_RETCODE_POSITION_CLOSED: "Position already closed",
            mt5.TRADE_RETCODE_INVALID_STOPS: "Invalid stops",
            mt5.TRADE_RETCODE_NO_CHANGES: "No changes",
            mt5.TRADE_RETCODE_SERVER_DISABLES_AT: "Server disables autotrading",
            mt5.TRADE_RETCODE_CLIENT_DISABLES_AT: "Client disables autotrading",
            mt5.TRADE_RETCODE_LOCKED: "Request locked",
            mt5.TRADE_RETCODE_FROZEN: "Order or position frozen",
            mt5.TRADE_RETCODE_INVALID_FILL: "Invalid order filling type",
            mt5.TRADE_RETCODE_ONLY_REAL: "Only real accounts",
            mt5.TRADE_RETCODE_LIMIT_ORDERS: "Limit orders",
            mt5.TRADE_RETCODE_LIMIT_VOLUME: "Limit volume",
            mt5.TRADE_RETCODE_INVALID_ORDER: "Invalid order",
            mt5.TRADE_RETCODE_POSITION_CLOSED: "Position already closed"
        }
        return retcodes.get(retcode, f"Unknown code {retcode}")
    
    def log_mt5_init(self, login, password, server):
        """MT5 başlatma sürecini detaylı logla"""
        md_logger.info("=" * 60)
        md_logger.info("MT5 INITIALIZATION DIAGNOSTIC")
        md_logger.info("=" * 60)
        
        # Initialize
        if not mt5.initialize():
            error = mt5.last_error()
            md_logger.error(f"MT5 init failed - Error Code: {error[0]}, Description: {error[1]}")
            return False
        
        md_logger.info("✓ MT5 initialized successfully")
        
        # Terminal info
        terminal_info = mt5.terminal_info()
        if terminal_info:
            md_logger.info("TERMINAL INFO:")
            md_logger.info(f"  - Connected: {terminal_info.connected}")
            md_logger.info(f"  - Path: {terminal_info.path}")
            md_logger.info(f"  - Data path: {terminal_info.data_path}")
            md_logger.info(f"  - Company: {terminal_info.company}")
            md_logger.info(f"  - Name: {terminal_info.name}")
            md_logger.info(f"  - Language: {terminal_info.language}")
            md_logger.info(f"  - Build: {terminal_info.build}")
            md_logger.info(f"  - Community account: {terminal_info.community_account}")
            md_logger.info(f"  - Community connection: {terminal_info.community_connection}")
            md_logger.info(f"  - Trade allowed: {terminal_info.trade_allowed}")
            md_logger.info(f"  - Email enabled: {terminal_info.email_enabled}")
            md_logger.info(f"  - FTP enabled: {terminal_info.ftp_enabled}")
        
        # Login
        if not mt5.login(login, password, server):
            error = mt5.last_error()
            md_logger.error(f"MT5 login failed - Error Code: {error[0]}, Description: {error[1]}")
            return False
        
        md_logger.info(f"✓ MT5 logged in successfully - Login: {login}, Server: {server}")
        
        # Account info
        account_info = mt5.account_info()
        if account_info:
            md_logger.info("ACCOUNT INFO:")
            md_logger.info(f"  - Login: {account_info.login}")
            md_logger.info(f"  - Server: {account_info.server}")
            md_logger.info(f"  - Currency: {account_info.currency}")
            md_logger.info(f"  - Balance: {account_info.balance}")
            md_logger.info(f"  - Equity: {account_info.equity}")
            md_logger.info(f"  - Profit: {account_info.profit}")
            md_logger.info(f"  - Margin: {account_info.margin}")
            md_logger.info(f"  - Free margin: {account_info.margin_free}")
            md_logger.info(f"  - Margin level: {account_info.margin_level}")
            md_logger.info(f"  - Leverage: {account_info.leverage}")
            md_logger.info(f"  - Trade mode: {account_info.trade_mode}")
            md_logger.info(f"  - Trade allowed: {account_info.trade_allowed}")
            md_logger.info(f"  - Trade expert: {account_info.trade_expert}")
            md_logger.info(f"  - Limit orders: {account_info.limit_orders}")
            md_logger.info(f"  - Margin SO mode: {account_info.margin_so_mode}")
        
        # Symbol mapping
        try:
            # copy_trading içinden SYMBOL_MAP'i çalışma anında çeker
            from copy_trading import SYMBOL_MAP  # noqa
            md_logger.info("CHECKING MAPPED SYMBOLS:")
            for darkex_sym, mt5_sym in SYMBOL_MAP.items():
                md_logger.info(f"  Checking {darkex_sym} → {mt5_sym}:")
                
                # Select symbol
                if not mt5.symbol_select(mt5_sym, True):
                    md_logger.warning(f"    ✗ Failed to select {mt5_sym}")
                    continue
                
                # Symbol info
                symbol_info = mt5.symbol_info(mt5_sym)
                if symbol_info:
                    md_logger.info(f"    ✓ Symbol selected: {mt5_sym}")
                    md_logger.info(f"      - Bid: {symbol_info.bid}")
                    md_logger.info(f"      - Ask: {symbol_info.ask}")
                    md_logger.info(f"      - Spread: {symbol_info.spread}")
                    md_logger.info(f"      - Volume min: {symbol_info.volume_min}")
                    md_logger.info(f"      - Volume max: {symbol_info.volume_max}")
                    md_logger.info(f"      - Volume step: {symbol_info.volume_step}")
                    md_logger.info(f"      - Contract size: {symbol_info.trade_contract_size}")
                    md_logger.info(f"      - Tick size: {symbol_info.trade_tick_size}")
                    md_logger.info(f"      - Tick value: {symbol_info.trade_tick_value}")
                    md_logger.info(f"      - Digits: {symbol_info.digits}")
                    md_logger.info(f"      - Trade mode: {symbol_info.trade_mode}")
                else:
                    md_logger.warning(f"    ✗ No info for {mt5_sym}")
        except ImportError:
            md_logger.warning("Could not import SYMBOL_MAP from copy_trading")
        
        # Check existing positions
        positions = mt5.positions_get()
        md_logger.info(f"EXISTING POSITIONS: {len(positions) if positions else 0}")
        if positions:
            for pos in positions:
                md_logger.info(f"  - {pos.symbol} {'BUY' if pos.type==0 else 'SELL'} Vol:{pos.volume} Profit:{pos.profit} Ticket:{pos.ticket}")
        
        md_logger.info("=" * 60)
        md_logger.info("MT5 INITIALIZATION DIAGNOSTIC COMPLETED")
        md_logger.info("=" * 60)
        return True
    
    def log_order_event(self, event_type, event_data, request=None, result=None):
        """Order event'lerini detaylı logla"""
        md_logger.info("=" * 40)
        md_logger.info(f"MT5 ORDER EVENT: {event_type}")
        md_logger.info(f"Event Data: {event_data}")
        
        if request:
            md_logger.info("Request Details:")
            for key, value in request.items():
                md_logger.info(f"  - {key}: {value}")
        
        if result:
            md_logger.info("Result Details:")
            md_logger.info(f"  - Retcode: {result.retcode} ({self.get_retcode_description(result.retcode)})")
            md_logger.info(f"  - Deal: {result.deal}")
            md_logger.info(f"  - Order: {result.order}")
            md_logger.info(f"  - Volume: {result.volume}")
            md_logger.info(f"  - Price: {result.price}")
            md_logger.info(f"  - Request ID: {result.request_id}")
            
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                md_logger.info("✅ ORDER SUCCESSFUL")
            else:
                md_logger.error("❌ ORDER FAILED")
        
        md_logger.info("=" * 40)
    
    def log_mt5_positions(self):
        """Mevcut MT5 pozisyonlarını logla"""
        positions = mt5.positions_get()
        if positions:
            md_logger.info(f"MT5 POSITIONS CHECK - Count: {len(positions)}")
            for pos in positions:
                md_logger.info(f"  MT5: {pos.symbol} {'BUY' if pos.type==0 else 'SELL'} "
                          f"Vol:{pos.volume} Open:{pos.price_open} Current:{pos.price_current} "
                          f"Profit:{pos.profit} Ticket:{pos.ticket} Magic:{pos.magic} "
                          f"Time:{datetime.fromtimestamp(pos.time)}")
        else:
            md_logger.info("MT5 POSITIONS CHECK - No positions found")
    
    def check_mt5_connection(self):
        """MT5 bağlantı durumunu kontrol et"""
        print("\n=== MT5 CONNECTION CHECK ===")
        
        # Terminal info
        info = mt5.terminal_info()
        if info:
            print(f"✓ Terminal connected: {info.connected}")
            print(f"✓ Path: {info.path}")
            print(f"✓ Data path: {info.data_path}")
            print(f"✓ Build: {info.build}")
        else:
            print("✗ Terminal info not available")
            
        # Account info
        account = mt5.account_info()
        if account:
            print(f"\n✓ Account connected: {account.login}")
            print(f"✓ Server: {account.server}")
            print(f"✓ Balance: {account.balance}")
            print(f"✓ Equity: {account.equity}")
            print(f"✓ Margin: {account.margin}")
            print(f"✓ Free margin: {account.margin_free}")
            print(f"✓ Leverage: {account.leverage}")
        else:
            print("✗ Account info not available")
            
        return info is not None and account is not None
    
    def get_mt5_positions_detailed(self):
        """MT5 pozisyonlarını detaylı al"""
        positions = mt5.positions_get()
        if positions is None:
            return []
        
        detailed_positions = []
        for pos in positions:
            # Position details
            details = {
                'ticket': pos.ticket,
                'time': datetime.fromtimestamp(pos.time).strftime('%Y-%m-%d %H:%M:%S'),
                'time_update': datetime.fromtimestamp(pos.time_update).strftime('%Y-%m-%d %H:%M:%S'),
                'time_msc': pos.time_msc,
                'time_update_msc': pos.time_update_msc,
                'type': 'BUY' if pos.type == 0 else 'SELL',
                'magic': pos.magic,
                'identifier': pos.identifier,
                'reason': pos.reason,
                'symbol': pos.symbol,
                'volume': pos.volume,
                'price_open': pos.price_open,
                'sl': pos.sl,
                'tp': pos.tp,
                'price_current': pos.price_current,
                'swap': pos.swap,
                'profit': pos.profit,
                'external_id': pos.external_id if hasattr(pos, 'external_id') else None
            }
            detailed_positions.append(details)
            
        return detailed_positions
    
    def monitor_position_changes(self, interval=1):
        """Pozisyon değişikliklerini sürekli izle"""
        print("\n=== MONITORING MT5 POSITION CHANGES ===")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                current_positions = {p['ticket']: p for p in self.get_mt5_positions_detailed()}
                current_time = datetime.now().strftime('%H:%M:%S')
                
                # Yeni pozisyonlar
                for ticket, pos in current_positions.items():
                    if ticket not in self.last_positions:
                        print(f"\n🟢 [{current_time}] NEW POSITION DETECTED:")
                        self._print_position_details(pos)
                        self.position_history.append({
                            'time': current_time,
                            'event': 'OPENED',
                            'position': pos
                        })
                
                # Kapanan pozisyonlar
                for ticket, pos in self.last_positions.items():
                    if ticket not in current_positions:
                        print(f"\n🔴 [{current_time}] POSITION CLOSED:")
                        self._print_position_details(pos)
                        self.position_history.append({
                            'time': current_time,
                            'event': 'CLOSED',
                            'position': pos
                        })
                
                # Güncellenen pozisyonlar (volume, profit değişimi)
                for ticket, pos in current_positions.items():
                    if ticket in self.last_positions:
                        old_pos = self.last_positions[ticket]
                        if (pos['volume'] != old_pos['volume'] or 
                            abs(pos['profit'] - old_pos['profit']) > 0.01):
                            print(f"\n🟡 [{current_time}] POSITION UPDATED:")
                            print(f"  Ticket: {ticket}")
                            print(f"  Volume: {old_pos['volume']} → {pos['volume']}")
                            print(f"  Profit: {old_pos['profit']:.2f} → {pos['profit']:.2f}")
                
                self.last_positions = current_positions
                time.sleep(0.2)
                
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
            self._show_position_history()
    
    def _print_position_details(self, pos):
        """Pozisyon detaylarını yazdır"""
        print(f"  Ticket: {pos['ticket']}")
        print(f"  Symbol: {pos['symbol']}")
        print(f"  Type: {pos['type']}")
        print(f"  Volume: {pos['volume']}")
        print(f"  Open Price: {pos['price_open']}")
        print(f"  Current Price: {pos['price_current']}")
        print(f"  Profit: {pos['profit']:.2f}")
        print(f"  Magic: {pos['magic']}")
    
    def _show_position_history(self):
        """Pozisyon geçmişini göster"""
        if self.position_history:
            print("\n=== POSITION HISTORY ===")
            for event in self.position_history:
                print(f"\n[{event['time']}] {event['event']}:")
                self._print_position_details(event['position'])
    
    def compare_darkex_mt5_positions(self, darkex_positions_df):
        """Darkex ve MT5 pozisyonlarını karşılaştır"""
        print("\n=== POSITION COMPARISON: DARKEX vs MT5 ===")
        
        # MT5 pozisyonları
        mt5_positions = self.get_mt5_positions_detailed()
        mt5_df = pd.DataFrame(mt5_positions)
        
        # Symbol mapping için import
        from copy_trading import SYMBOL_MAP  # noqa
        
        # Darkex pozisyonlarını MT5 symbol'e map et
        darkex_mapped = []
        for _, pos in darkex_positions_df.iterrows():
            mt5_symbol = SYMBOL_MAP.get(pos['contractName'])
            if mt5_symbol:
                darkex_mapped.append({
                    'darkex_symbol': pos['contractName'],
                    'mt5_symbol': mt5_symbol,
                    'side': pos['side'],
                    'volume': pos['volume'],
                    'pnl': pos.get('uPnL', 0)
                })
        
        # Karşılaştırma tablosu
        comparison_data = []
        
        # Her Darkex pozisyonu için MT5'te karşılığını bul
        for dx_pos in darkex_mapped:
            mt5_match = None
            if not mt5_df.empty:
                # Symbol ve side'a göre eşleştir
                mt5_side = 'BUY' if dx_pos['side'] == 'BUY' else 'SELL'
                matches = mt5_df[(mt5_df['symbol'] == dx_pos['mt5_symbol']) & 
                                (mt5_df['type'] == mt5_side)]
                if not matches.empty:
                    mt5_match = matches.iloc[0]
            
            comparison_data.append({
                'Symbol': dx_pos['darkex_symbol'],
                'Side': dx_pos['side'],
                'Darkex Vol': f"{dx_pos['volume']:.4f}",
                'MT5 Vol': f"{mt5_match['volume']:.4f}" if mt5_match is not None else "NOT FOUND",
                'Darkex PnL': f"{dx_pos['pnl']:.2f}",
                'MT5 PnL': f"{mt5_match['profit']:.2f}" if mt5_match is not None else "N/A",
                'Status': '✓' if mt5_match is not None else '✗'
            })
        
        # MT5'te olup Darkex'te olmayan pozisyonlar
        if not mt5_df.empty:
            for _, mt5_pos in mt5_df.iterrows():
                # Darkex'te karşılığı var mı kontrol et
                found = False
                for dx_mapped in darkex_mapped:
                    if (dx_mapped['mt5_symbol'] == mt5_pos['symbol'] and
                        dx_mapped['side'] == ('BUY' if mt5_pos['type'] == 'BUY' else 'SELL')):
                        found = True
                        break
                
                if not found:
                    comparison_data.append({
                        'Symbol': mt5_pos['symbol'],
                        'Side': mt5_pos['type'],
                        'Darkex Vol': "NOT FOUND",
                        'MT5 Vol': f"{mt5_pos['volume']:.4f}",
                        'Darkex PnL': "N/A",
                        'MT5 PnL': f"{mt5_pos['profit']:.2f}",
                        'Status': '⚠️ ORPHAN'
                    })
        
        # Tabloyu yazdır
        if comparison_data:
            print("\n" + "-"*100)
            print(f"{'Symbol':<20} {'Side':<6} {'Darkex Vol':<12} {'MT5 Vol':<12} {'Darkex PnL':<12} {'MT5 PnL':<12} {'Status':<10}")
            print("-"*100)
            for row in comparison_data:
                print(f"{row['Symbol']:<20} {row['Side']:<6} {row['Darkex Vol']:<12} {row['MT5 Vol']:<12} {row['Darkex PnL']:<12} {row['MT5 PnL']:<12} {row['Status']:<10}")
            print("-"*100)
        else:
            print("No positions to compare")
        
        # Özet
        print(f"\n📊 SUMMARY:")
        print(f"Darkex positions: {len(darkex_mapped)}")
        print(f"MT5 positions: {len(mt5_positions)}")
        matched = sum(1 for d in comparison_data if d['Status'] == '✓')
        print(f"Matched positions: {matched}")
        print(f"Sync rate: {matched/max(len(darkex_mapped), 1)*100:.1f}%")
    
    def test_symbol_info(self):
        """MT5 symbol bilgilerini test et"""
        print("\n=== MT5 SYMBOL INFO TEST ===")
        from copy_trading import SYMBOL_MAP  # noqa
        
        for darkex_sym, mt5_sym in SYMBOL_MAP.items():
            print(f"\n{darkex_sym} → {mt5_sym}:")
            
            # Symbol seç
            if not mt5.symbol_select(mt5_sym, True):
                print(f"  ✗ Failed to select symbol")
                continue
                
            # Symbol info
            info = mt5.symbol_info(mt5_sym)
            if info:
                print(f"  ✓ Bid: {info.bid}")
                print(f"  ✓ Ask: {info.ask}")
                print(f"  ✓ Volume min: {info.volume_min}")
                print(f"  ✓ Volume max: {info.volume_max}")
                print(f"  ✓ Volume step: {info.volume_step}")
                print(f"  ✓ Contract size: {info.trade_contract_size}")
                print(f"  ✓ Tick size: {info.trade_tick_size}")
                print(f"  ✓ Tick value: {info.trade_tick_value}")
                print(f"  ✓ Digits: {info.digits}")
                print(f"  ✓ Spread: {info.spread}")
            else:
                print(f"  ✗ Symbol info not available")
            
            # Tick info
            tick = mt5.symbol_info_tick(mt5_sym)
            if tick:
                print(f"  ✓ Last tick time: {datetime.fromtimestamp(tick.time)}")
            else:
                print(f"  ✗ Tick info not available")
    
    def get_mt5_positions_with_notifications(self, telegram_token, chat_id):
        """MT5 pozisyonlarını al ve bildirim gönder"""
        try:
            import requests
            positions = mt5.positions_get()
            
            if not positions:
                msg = "📭 *MT5: No open positions*"
                url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
                data = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
                requests.post(url, data=data, timeout=5)
                return []
            
            msg = "🔵 *MT5 POSITIONS:*\n\n"
            for pos in positions:
                side = "BUY" if pos.type == 0 else "SELL"
                emoji = "🟢" if pos.profit > 0 else "🔴" if pos.profit < 0 else "⚪"
                
                msg += (
                    f"{emoji} `{pos.symbol}` | `{side}`\n"
                    f"├─ Volume: `{pos.volume:.4f}`\n"
                    f"├─ Entry: `{pos.price_open:.2f}` | Current: `{pos.price_current:.2f}`\n"
                    f"├─ PnL: `{pos.profit:+.2f}` | Swap: `{pos.swap:+.2f}`\n"
                    f"├─ Ticket: `{pos.ticket}` | Magic: `{pos.magic}`\n"
                    f"└─ Time: `{datetime.fromtimestamp(pos.time).strftime('%Y-%m-%d %H:%M')}`\n\n"
                )
            
            # Telegram mesaj limiti için böl
            if len(msg) > 3000:
                chunks = [msg[i:i+3000] for i in range(0, len(msg), 3000)]
                for chunk in chunks:
                    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
                    data = {"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"}
                    requests.post(url, data=data, timeout=5)
            else:
                url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
                data = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
                requests.post(url, data=data, timeout=5)
            
            return positions
            
        except Exception as e:
            md_logger.error(f"get_mt5_positions_with_notifications error: {e}")
            return []
    
    def compare_positions_detailed(self, darkex_positions_df, telegram_token, chat_id):
        """Darkex ve MT5 pozisyonlarını detaylı karşılaştır ve bildir"""
        try:
            import requests
            from copy_trading import SYMBOL_MAP  # noqa
            
            mt5_positions = mt5.positions_get() or []
            
            # Özet bilgi
            msg = "🔄 *POSITION COMPARISON*\n"
            msg += f"Darkex: {len(darkex_positions_df)} positions\n"
            msg += f"MT5: {len(mt5_positions)} positions\n\n"
            
            # Darkex pozisyonları için MT5 eşleştirme
            msg += "📊 *DETAILED COMPARISON:*\n\n"
            
            for _, dx_pos in darkex_positions_df.iterrows():
                mt5_symbol = SYMBOL_MAP.get(dx_pos['contractName'])
                mt5_match = None
                
                if mt5_symbol:
                    for mt5_pos in mt5_positions:
                        mt5_side = 'BUY' if mt5_pos.type == 0 else 'SELL'
                        if mt5_pos.symbol == mt5_symbol and mt5_side == dx_pos['side']:
                            mt5_match = mt5_pos
                            break
                
                sync_icon = "✅" if mt5_match else "❌"
                
                msg += f"{sync_icon} *{dx_pos['contractName']}* - {dx_pos['side']}\n"
                msg += f"├─ Darkex: Vol `{dx_pos['volume']:.4f}` | PnL `{dx_pos.get('uPnL', 0):+.2f}`\n"
                
                if mt5_match:
                    msg += f"└─ MT5: Vol `{mt5_match.volume:.4f}` | PnL `{mt5_match.profit:+.2f}` | #{mt5_match.ticket}\n"
                    
                    # Volume uyuşmazlığı kontrolü
                    expected_mt5_vol = dx_pos['volume'] * 0.001  # CONTRACT_SIZE
                    vol_diff = abs(mt5_match.volume - expected_mt5_vol)
                    if vol_diff > 0.0001:
                        msg += f"    ⚠️ Volume mismatch! Expected: `{expected_mt5_vol:.4f}`\n"
                else:
                    msg += f"└─ MT5: NOT FOUND ⚠️\n"
                
                msg += "\n"
            
            # MT5'te olup Darkex'te olmayan pozisyonlar
            orphan_positions = []
            for mt5_pos in mt5_positions:
                found = False
                mt5_side = 'BUY' if mt5_pos.type == 0 else 'SELL'
                
                # Darkex eşleştirme
                try:
                    from copy_trading import SYMBOL_MAP as _SM
                except Exception:
                    _SM = {}
                for darkex_sym, mt5_sym in _SM.items():
                    if mt5_sym == mt5_pos.symbol:
                        # Bu symbol Darkex'te var mı?
                        dx_match = darkex_positions_df[
                            (darkex_positions_df['contractName'] == darkex_sym) & 
                            (darkex_positions_df['side'] == mt5_side)
                        ] if darkex_positions_df is not None else pd.DataFrame()
                        if not dx_match.empty:
                            found = True
                            break
                if not found:
                    orphan_positions.append(mt5_pos)
            
            if orphan_positions:
                msg += "⚠️ *ORPHAN MT5 POSITIONS:*\n"
                for pos in orphan_positions:
                    side = 'BUY' if pos.type == 0 else 'SELL'
                    msg += f"└─ {pos.symbol} {side} Vol:{pos.volume:.4f} #{pos.ticket}\n"
            
            # Sync durumu özeti
            synced_count = 0
            try:
                from copy_trading import SYMBOL_MAP as _SM2
            except Exception:
                _SM2 = {}
            for _, dx_pos in darkex_positions_df.iterrows():
                if any(mt5_pos.symbol == _SM2.get(dx_pos['contractName']) and 
                       ('BUY' if mt5_pos.type == 0 else 'SELL') == dx_pos['side'] 
                       for mt5_pos in mt5_positions):
                    synced_count += 1
            
            sync_rate = (synced_count / len(darkex_positions_df) * 100) if len(darkex_positions_df) > 0 else 0
            msg += f"\n📈 *SYNC RATE:* {sync_rate:.1f}% ({synced_count}/{len(darkex_positions_df)})"
            
            # Mesajı gönder
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            data = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
            requests.post(url, data=data, timeout=5)
            
        except Exception as e:
            md_logger.error(f"compare_positions_detailed error: {e}")
            import requests
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            data = {"chat_id": chat_id, "text": f"❌ Comparison error: {e}", "parse_mode": "Markdown"}
            requests.post(url, data=data, timeout=5)

# Telegram bot'a entegre edilecek diagnostic komutları
def handle_diagnostic(update):
    """Telegram'dan /diagnostic komutu"""
    # Bu fonksiyon çalışma anında telegram_bot'tan import eder
    from telegram_bot import send_text_to_telegram, TELEGRAM_BOT_TOKEN  # noqa
    cid = update["message"]["chat"]["id"]
    
    diag = MT5Diagnostic()
    
    # Connection check
    if diag.check_mt5_connection():
        msg = "✅ MT5 Connection OK"
    else:
        msg = "❌ MT5 Connection FAILED"
    
    # Get current positions
    positions = diag.get_mt5_positions_detailed()
    msg += f"\n\n📊 Current MT5 Positions: {len(positions)}"
    
    for pos in positions[:3]:  # İlk 3 pozisyon
        msg += f"\n• {pos['symbol']} {pos['type']} Vol:{pos['volume']:.4f} PnL:{pos['profit']:.2f}"
    
    send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, msg)


def handle_validate_sync(update):
    """Darkex ve MT5 senkronizasyonunu doğrula"""
    from telegram_bot import send_text_to_telegram, TELEGRAM_BOT_TOKEN  # noqa
    from copy_trading import get_open_positions_df  # noqa
    
    cid = update["message"]["chat"]["id"]
    
    try:
        diag = MT5Diagnostic()
        darkex_df = get_open_positions_df()
        
        # Basit özet için
        mt5_positions = diag.get_mt5_positions_detailed()
        
        msg = f"🔄 *SYNC VALIDATION*\n\n"
        msg += f"Darkex Positions: {len(darkex_df)}\n"
        msg += f"MT5 Positions: {len(mt5_positions)}\n"
        
        if len(darkex_df) == len(mt5_positions):
            msg += "✅ Count matches!"
        else:
            msg += f"⚠️ Count mismatch! Diff: {abs(len(darkex_df) - len(mt5_positions))}"
        
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, msg)
        
    except Exception as e:
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, f"❌ Validation error: {e}")

# mt5_diagnostic modül alias (tek dosya import uyumluluğu)
sys.modules['mt5_diagnostic'] = sys.modules[__name__]
# ---------------------------
# mt5_diagnostic.py (bitiş)
# ---------------------------


# ---------------------------
# copy_trading.py (başlangıç)
# ---------------------------
import time as _time_ct
import threading as _threading_ct
import queue as _queue_ct
import logging as _logging_ct
import math as _math_ct
from urllib.parse import urlencode as _urlencode_ct
from datetime import datetime as _dt_ct, timedelta as _td_ct

import requests as _requests_ct
import pandas as _pd_ct
import MetaTrader5 as _mt5_ct

# MT5 Diagnostic import
from mt5_diagnostic import MT5Diagnostic as _MT5DiagClass

# Global diagnostic instance
mt5_diag = _MT5DiagClass()

# --- Bot durumu takibi için ---
bot_status = {
    'darkex_connected':   False,
    'mt5_connected':      False,
    'monitoring_active':  False,
    'last_update':        None,
    'start_time':         _dt_ct.now()
}

# --- Logging setup ---
_logging_ct.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=_logging_ct.INFO
)
logger = _logging_ct.getLogger(__name__)

# === Configuration ===
TELEGRAM_BOT_TOKEN = "7895901821:AAEJs3mmWxiWrRyVKcRiAxMN2Rn4IpiyV0o"
CHAT_ID            = "-4678220102"

DARKEX_API_KEY     = "cb658cb61b423d35be9bf855e21a34e7"
DARKEX_SECRET_KEY  = "638beeeb2ac75e229cf716202fc8da6c"
DARKEX_BASE_URL    = "https://futuresopenapi.darkex.com"

MT5_LOGIN     = 810005950
MT5_PASSWORD  = "nDGbT_4O"
MT5_SERVER    = "Taurex-Demo"
MT5_LEVERAGE  = 1

# Darkex symbol → MT5 symbol
SYMBOL_MAP = {
    "USDT1791-BTC-USDT": "BTCUSD",
    "USDT1791-ETH-USDT": "ETHUSD",
    "USDT1791-SOL-USDT": "SOLUSD",
}

# Contract size per Darkex symbol
CONTRACT_SIZE = {
    "USDT1791-BTC-USDT": 0.001,
    "USDT1791-ETH-USDT": 0.001,
    "USDT1791-SOL-USDT": 0.00001,
}

mt5_queue = _queue_ct.Queue()

# === DARKEX REST helpers ===
def generate_signature(timestamp, method, request_path, body, secret_key):
    pre = timestamp + method.upper() + request_path + (body or "")
    import hmac, hashlib
    sig = hmac.new(secret_key.encode(), pre.encode(), hashlib.sha256).hexdigest()
    return _requests_ct.utils.quote(_requests_ct.utils.quote(sig))

def get_futures_account_info():
    try:
        path = "/fapi/v1/account"
        ts   = str(int(_time_ct.time() * 1000))
        sig  = generate_signature(ts, "GET", path, "", DARKEX_SECRET_KEY)
        headers = {
            "X-CH-APIKEY": DARKEX_API_KEY,
            "X-CH-TS":     ts,
            "Content-Type":"application/json",
            "X-CH-SIGN":   sig
        }
        r = _requests_ct.get(DARKEX_BASE_URL + path, headers=headers, timeout=10)
        r.raise_for_status()
        bot_status['darkex_connected'] = True
        logger.info("Futures Account Info: %s %s", r.status_code, r.text)
        return r.json()
    except Exception as e:
        bot_status['darkex_connected'] = False
        logger.error("Darkex API error: %s", e)
        raise

def get_futures_account_balance_df():
    try:
        df = _pd_ct.DataFrame(get_futures_account_info().get("account", []))
        if not df.empty and "totalEquity" in df.columns:
            return df[["totalEquity"]]
    except Exception as e:
        logger.error("get_futures_account_balance_df error: %s", e)
    return _pd_ct.DataFrame()

def get_open_positions_df():
    path = "/fapi/v1/account"
    ts   = str(int(_time_ct.time() * 1000))
    sig  = generate_signature(ts, "GET", path, "", DARKEX_SECRET_KEY)
    headers = {
        "X-CH-APIKEY": DARKEX_API_KEY,
        "X-CH-TS":     ts,
        "Content-Type":"application/json",
        "X-CH-SIGN":   sig
    }
    r = _requests_ct.get(DARKEX_BASE_URL + path, headers=headers, timeout=10)
    r.raise_for_status()
    rows = []
    for acct in r.json().get("account", []):
        for vo in acct.get("positionVos", []):
            cname = vo.get("contractName")
            for pos in vo.get("positions", []):
                vol = float(pos.get("volume", 0))
                if vol <= 0:
                    continue
                rows.append({
                    "contractName":       cname,
                    "side":               pos.get("side"),
                    "volume":             vol,
                    "openPrice":          float(pos.get("openPrice", 0)),
                    "uPnL":               float(pos.get("unRealizedAmount", 0)),
                    "tradeFee":           float(pos.get("tradeFee", 0)),
                    "openRealizedAmount": float(pos.get("openRealizedAmount", 0)),
                    "leverageLevel":      pos.get("leverageLevel"),
                    "ctime":              pos.get("ctime")
                })
    return _pd_ct.DataFrame(rows)

def get_open_orders_df():
    try:
        path = "/fapi/v1/openOrders"
        ts   = str(int(_time_ct.time() * 1000))
        sig  = generate_signature(ts, "GET", path, "", DARKEX_SECRET_KEY)
        headers = {
            "X-CH-APIKEY": DARKEX_API_KEY,
            "X-CH-TS":     ts,
            "Content-Type":"application/json",
            "X-CH-SIGN":   sig
        }
        r = _requests_ct.get(DARKEX_BASE_URL + path, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            orders = data.get("data", {}).get("orders", [])
        else:
            orders = []
        rows = []
        for o in orders:
            rows.append({
                "contractName": o.get("contractName"),
                "side":         o.get("side"),
                "price":        float(o.get("price", 0)),
                "origQty":      float(o.get("quantity", 0)),
                "type":         o.get("type"),
                "status":       o.get("status"),
                "orderId":      o.get("orderId")
            })
        return _pd_ct.DataFrame(rows)
    except Exception as e:
        logger.error("get_open_orders_df error: %s", e)
        return _pd_ct.DataFrame()

# === Telegram helper (alias) ===
def send_text_to_telegram(token, chat_id, text):
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode":"Markdown"}
    try:
        r = _requests_ct.post(url, data=data, timeout=5)
        r.raise_for_status()
    except Exception as e:
        logger.error("Telegram send failed: %s", e)

# === MT5 Integration ===
def init_mt5(login, password, server):
    try:
        # Diagnostic log başlat
        success = mt5_diag.log_mt5_init(login, password, server)
        if not success:
            bot_status['mt5_connected'] = False
            return False
            
        # Original logic devam ediyor
        if not _mt5_ct.initialize():
            raise RuntimeError(f"MT5 init failed: {_mt5_ct.last_error()}")
        if not _mt5_ct.login(login, password, server):
            raise RuntimeError(f"MT5 login failed: {_mt5_ct.last_error()}")
        bot_status['mt5_connected'] = True
        logger.info("MT5 initialized and logged in")
        return True
    except Exception as e:
        bot_status['mt5_connected'] = False
        logger.error("MT5 init error: %s", e)
        return False

def volume_to_lots(darkex_symbol, contract_count):
    mt5_sym = SYMBOL_MAP.get(darkex_symbol)
    if not mt5_sym:
        raise ValueError(f"No mapping for {darkex_symbol}")
    info = _mt5_ct.symbol_info(mt5_sym)
    if info is None:
        raise RuntimeError(f"No symbol info for {mt5_sym}")
    raw = contract_count * CONTRACT_SIZE[darkex_symbol] * MT5_LEVERAGE / info.trade_contract_size
    step, vol_min = info.volume_step, info.volume_min
    lots = _math_ct.ceil(raw / step) * step
    return max(lots, vol_min)

def mt5_executor():
    for s in SYMBOL_MAP.values():
        _mt5_ct.symbol_select(s, True)
    tickets = {}
    logger.info("MT5 executor started")
    while True:
        try:
            evt = mt5_queue.get()
            sym = SYMBOL_MAP.get(evt['symbol'])
            if not sym:
                logger.error(f"No mapping for {evt['symbol']}")
                continue

            if evt['action']=='open':
                cnt  = evt['contracts']
                lots = volume_to_lots(evt['symbol'], cnt)
                tick = _mt5_ct.symbol_info_tick(sym)
                if tick is None:
                    logger.error(f"No tick for {sym}")
                    continue
                price = tick.ask if evt['side']=='BUY' else tick.bid
                req = {
                    'action': _mt5_ct.TRADE_ACTION_DEAL,
                    'symbol': sym,
                    'volume': lots,
                    'type':   _mt5_ct.ORDER_TYPE_BUY if evt['side']=='BUY' else _mt5_ct.ORDER_TYPE_SELL,
                    'price':  price,
                    'deviation':20,
                    'magic':123456,
                    'comment':"MBD2025",
                    'type_time':_mt5_ct.ORDER_TIME_GTC,
                    'type_filling':_mt5_ct.ORDER_FILLING_IOC,
                }
                res = _mt5_ct.order_send(req)
                if res.retcode==_mt5_ct.TRADE_RETCODE_DONE:
                    key = f"{evt['symbol']}|{evt['side']}"
                    tickets.setdefault(key, []).append(res.order)
                    logger.info(f"✅ OPENED {sym} lots={lots} ticket={res.order}")
                else:
                    logger.error(f"❌ OPEN failed: {res.comment}")

            else:  # close
                rem = evt['contracts']
                key = f"{evt['symbol']}|{evt['side']}"
                ts = tickets.get(key, [])
                if not ts:
                    logger.error(f"No tickets to close for {key}")
                    continue
                info = _mt5_ct.symbol_info(sym)
                lev  = MT5_LEVERAGE
                tick = _mt5_ct.symbol_info_tick(sym)
                new_ts = []
                for tkt in ts:
                    if rem<=0:
                        new_ts.append(tkt)
                        continue
                    pos = next((p for p in _mt5_ct.positions_get(symbol=sym) if p.ticket==tkt), None)
                    if not pos: continue
                    avail = pos.volume * info.trade_contract_size / CONTRACT_SIZE[evt['symbol']] / lev
                    to_close = min(rem, avail)
                    lots = min(volume_to_lots(evt['symbol'], to_close), pos.volume)
                    close_type = _mt5_ct.ORDER_TYPE_SELL if pos.type==0 else _mt5_ct.ORDER_TYPE_BUY
                    price = tick.bid if close_type==_mt5_ct.ORDER_TYPE_SELL else tick.ask
                    creq = {
                        'action':_mt5_ct.TRADE_ACTION_DEAL,
                        'symbol':sym,
                        'volume':lots,
                        'type':close_type,
                        'position':tkt,
                        'price':price,
                        'deviation':20,
                        'magic':123456,
                        'comment':"MBD2025",
                        'type_time':_mt5_ct.ORDER_TIME_GTC,
                        'type_filling':_mt5_ct.ORDER_FILLING_IOC,
                    }
                    cres = _mt5_ct.order_send(creq)
                    if cres.retcode==_mt5_ct.TRADE_RETCODE_DONE:
                        logger.info(f"✅ CLOSED ticket={tkt} vol={lots}")
                        rem -= to_close
                        if avail>to_close:
                            new_ts.append(tkt)
                    else:
                        logger.error(f"❌ CLOSE failed: {cres.comment}")
                        new_ts.append(tkt)
                if new_ts:
                    tickets[key] = new_ts
                else:
                    tickets.pop(key, None)

        except Exception as e:
            logger.error(f"mt5_executor error: {e}")
            _time_ct.sleep(0.2)

# === Enhanced Position Monitoring ===
def _pos_key(pos):
    return f"{pos['contractName']}|{pos['side']}"

def send_position_opened_notification(pos):
    # Darkex bildirimi
    msg = (
f"🚀 *NEW POSITION OPENED* 🚀\n\n"
f"📈 Darkex: `{pos['contractName']}` | `{pos['side']}` | Vol: `{pos['volume']}`\n"
f"Entry: `{pos['openPrice']:.2f}` | Lev: `{pos.get('leverageLevel',0)}x`\n"
f"⏰ `{_dt_ct.now().strftime('%H:%M:%S')}`"
    )
    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, msg)
    
    # MT5 pozisyonunu kontrol et ve bildir
    try:
        mt5_symbol = SYMBOL_MAP.get(pos['contractName'])
        if mt5_symbol:
            _time_ct.sleep(2)  # MT5'in pozisyon açması için kısa bekleme
            positions = _mt5_ct.positions_get(symbol=mt5_symbol)
            if positions:
                for mt5_pos in positions:
                    mt5_side = 'BUY' if mt5_pos.type == 0 else 'SELL'
                    if mt5_side == pos['side'] and mt5_pos.magic == 123456:
                        # Expected volume hesapla
                        expected_volume = pos['volume'] * CONTRACT_SIZE.get(pos['contractName'], 0.001)
                        
                        mt5_msg = (
f"🔵 *MT5 POSITION OPENED* 🔵\n\n"
f"📊 MT5: `{mt5_pos.symbol}` | `{mt5_side}` | Vol: `{mt5_pos.volume:.3f}`\n"
f"Expected Vol: `{expected_volume:.3f}` | Entry: `{mt5_pos.price_open:.2f}`\n"
f"Ticket: `#{mt5_pos.ticket}` | Magic: `{mt5_pos.magic}`\n"
f"⏰ `{_dt_ct.now().strftime('%H:%M:%S')}`"
                        )
                        send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, mt5_msg)
                        logger.info(f"MT5 Position Opened - Symbol: {mt5_pos.symbol}, Volume: {mt5_pos.volume}, Expected: {expected_volume}")
                        break
    except Exception as e:
        logger.error(f"MT5 position check error: {e}")

def send_position_updated_notification(pos, diff):
    typ = "INCREASED" if diff>0 else "DECREASED"
    emo = "📈" if diff>0 else "📉"
    
    # Darkex bildirimi
    msg = (
f"{emo} *POSITION {typ}* {emo}\n\n"
f"Symbol: `{pos['contractName']}` | `{pos['side']}`\n"
f"Change: `{diff:+.4f}` → New Vol: `{pos['volume']:+.4f}`\n"
f"PnL: `{pos.get('uPnL',0):+.2f}` | `{_dt_ct.now().strftime('%H:%M:%S')}`"
    )
    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, msg)
    
    # MT5 pozisyon güncelleme bildirimi
    try:
        mt5_symbol = SYMBOL_MAP.get(pos['contractName'])
        if mt5_symbol:
            _time_ct.sleep(2)  # MT5'in güncellenmesi için kısa bekleme
            positions = _mt5_ct.positions_get(symbol=mt5_symbol)
            if positions:
                total_mt5_volume = 0
                total_mt5_profit = 0
                position_count = 0
                
                for mt5_pos in positions:
                    mt5_side = 'BUY' if mt5_pos.type == 0 else 'SELL'
                    if mt5_side == pos['side'] and mt5_pos.magic == 123456:
                        total_mt5_volume += mt5_pos.volume
                        total_mt5_profit += mt5_pos.profit
                        position_count += 1
                
                if position_count > 0:
                    # Expected volume hesapla
                    expected_volume = pos['volume'] * CONTRACT_SIZE.get(pos['contractName'], 0.001)
                    
                    mt5_msg = (
f"{emo} *MT5 POSITION {typ}* {emo}\n\n"
f"Symbol: `{mt5_symbol}` | `{pos['side']}`\n"
f"Total Volume: `{total_mt5_volume:.3f}` (Expected: `{expected_volume:.3f}`)\n"
f"Positions: `{position_count}` | Total PnL: `{total_mt5_profit:+.2f}`\n"
f"⏰ `{_dt_ct.now().strftime('%H:%M:%S')}`"
                    )
                    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, mt5_msg)
                    logger.info(f"MT5 Position Updated - Total Volume: {total_mt5_volume}, Expected: {expected_volume}, Count: {position_count}")
    except Exception as e:
        logger.error(f"MT5 position update check error: {e}")

def send_position_closed_notification(pos):
    # Darkex bildirimi
    msg = (
f"🏁 *POSITION CLOSED* 🏁\n\n"
f"Symbol: `{pos['contractName']}` | `{pos['side']}`\n"
f"Final Vol: `{pos['volume']:+.4f}` | PnL: `{pos.get('uPnL',0):+.2f}`\n"
f"⏰ `{_dt_ct.now().strftime('%H:%M:%S')}`"
    )
    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, msg)
    
                # MT5 kapanan pozisyon bilgisi
    try:
        mt5_symbol = SYMBOL_MAP.get(pos['contractName'])
        if mt5_symbol:
            # Son kapatılan MT5 pozisyonunu bulmak için history'ye bak
            to_time = _dt_ct.now()
            from_time = to_time - _td_ct(minutes=5)
            
            # History deals al
            deals = _mt5_ct.history_deals_get(from_time, to_time, symbol=mt5_symbol)
            
            if deals:
                # Magic number 123456 olan ve OUT type (kapanış) deal'leri bul
                close_deals = []
                total_volume = 0
                total_profit = 0
                
                for deal in deals:
                    # Deal entry 1 = OUT (pozisyon kapanışı), 0 = IN (pozisyon açılışı)
                    if deal.magic == 123456 and deal.entry == 1:  
                        close_deals.append(deal)
                        total_volume += deal.volume
                        total_profit += deal.profit
                        
                        # Commission ve swap'ı da ekle
                        total_profit += deal.commission if hasattr(deal, 'commission') else 0
                        total_profit += deal.swap if hasattr(deal, 'swap') else 0
                
                if close_deals:
                    # En son kapanan deal
                    last_deal = close_deals[-1]
                    
                    # Darkex volume'ü MT5 volume'e çevir
                    expected_mt5_volume = pos['volume'] * CONTRACT_SIZE.get(pos['contractName'], 0.001)
                    
                    mt5_msg = (
f"🔴 *MT5 POSITION CLOSED* 🔴\n\n"
f"Symbol: `{mt5_symbol}` | Deal: `#{last_deal.order}`\n"
f"Volume: `{total_volume:.3f}` (Expected: `{expected_mt5_volume:.3f}`)\n"
f"Profit: `{total_profit:+.2f}` | Price: `{last_deal.price:.2f}`\n"
f"⏰ `{_dt_ct.now().strftime('%H:%M:%S')}`"
                    )
                    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, mt5_msg)
                    
                    # Debug log
                    logger.info(f"MT5 Close - Total Volume: {total_volume}, Total Profit: {total_profit}, Deals: {len(close_deals)}")
                    
    except Exception as e:
        logger.error(f"MT5 closed position check error: {e}")

def monitor_positions(poll_interval=10):
    bot_status['monitoring_active'] = True
    logger.info("🎯 Position monitoring started")
    prev = {}

    # NEW: Aynı (contractName|side) için son işlediğimiz pozisyon örneğinin ctime'ını tut
    last_open_ctime = {}  # key -> last ctime acted on (OPEN üretildi)

    while True:
        try:
            df   = get_open_positions_df()
            curr = {_pos_key(r): r for _,r in df.iterrows()}

            # Yeni / güncelleme
            for k,pos in curr.items():
                cnt = pos['volume']
                ctm = pos.get('ctime')  # NEW: pozisyon örnek kimliği

                if k not in prev:
                    # NEW: Aynı ctime ile daha önce OPEN üretmişsek TEKRAR AÇMA!
                    if ctm and last_open_ctime.get(k) == ctm:
                        logger.warning(f"⛔ Ignored replay OPEN for {k} (ctime={ctm})")
                        continue

                    send_position_opened_notification(pos)
                    mt5_queue.put({'symbol':pos['contractName'],'side':pos['side'],'contracts':cnt,'action':'open'})

                    # NEW: Bu örneği işlediğimizi kaydet
                    if ctm:
                        last_open_ctime[k] = ctm
                else:
                    old = prev[k]['volume']
                    if cnt>old:
                        diff=cnt-old
                        send_position_updated_notification(pos,diff)
                        mt5_queue.put({'symbol':pos['contractName'],'side':pos['side'],'contracts':diff,'action':'open'})
                    elif cnt<old:
                        diff=old-cnt
                        send_position_updated_notification(pos,-diff)
                        mt5_queue.put({'symbol':pos['contractName'],'side':pos['side'],'contracts':diff,'action':'close'})

            # Tamamıyla kapanan
            for k,pinfo in prev.items():
                if k not in curr:
                    send_position_closed_notification(pinfo)
                    mt5_queue.put({'symbol':pinfo['contractName'],'side':pinfo['side'],'contracts':pinfo['volume'],'action':'close'})

                    # NEW: Bu kapanan örneğin ctime'ını "son açılmış" olarak işaretle
                    ctm_closed = pinfo.get('ctime')
                    if ctm_closed:
                        last_open_ctime[k] = ctm_closed

            # prev güncelle (NEW: ctime ekliyoruz)
            prev = {
                k:{
                    'contractName':  p['contractName'],
                    'side':          p['side'],
                    'volume':        p['volume'],
                    'uPnL':          p.get('uPnL',0),
                    'leverageLevel': p.get('leverageLevel',0),
                    'ctime':         p.get('ctime')  # NEW
                } for k,p in curr.items()
            }

            bot_status['last_update'] = _dt_ct.now()

        except Exception as e:
            logger.error("monitor_positions error: %s", e)
            bot_status['monitoring_active']=False
            send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, f"⚠️ *MONITORING ERROR* ❌ {e}")
            _time_ct.sleep(0.3)
            bot_status['monitoring_active']=True

        _time_ct.sleep(poll_interval)

# copy_trading modül alias
sys.modules['copy_trading'] = sys.modules[__name__]
# ---------------------------
# copy_trading.py (bitiş)
# ---------------------------


# ---------------------------
# statistics.py (içerik: trading_statistics.py) (başlangıç)
# ---------------------------
import time as _time_st
import logging as _logging_st
import requests as _requests_st
import pandas as _pd_st
import MetaTrader5 as _mt5_st
from datetime import datetime as _dt_st, timedelta as _td_st
from typing import Dict as _Dict_st, List as _List_st
import hmac as _hmac_st
import hashlib as _hashlib_st

st_logger = _logging_st.getLogger(__name__)

class TradingStatistics:
    def __init__(self):
        self.darkex_cache = {}
        self.mt5_cache = {}
        self.last_update = None
        
        # Config - bu değerleri copy_trading.py'den alacağız
        self.DARKEX_API_KEY = None
        self.DARKEX_SECRET_KEY = None
        self.DARKEX_BASE_URL = None
        self.SYMBOL_MAP = {}
        
    def set_config(self, api_key, secret_key, base_url, symbol_map):
        """Config ayarlarını set et"""
        self.DARKEX_API_KEY = api_key
        self.DARKEX_SECRET_KEY = secret_key
        self.DARKEX_BASE_URL = base_url
        self.SYMBOL_MAP = symbol_map
    
    def generate_signature(self, timestamp, method, request_path, body, secret_key):
        """Darkex signature generate et"""
        pre = timestamp + method.upper() + request_path + (body or "")
        sig = _hmac_st.new(secret_key.encode(), pre.encode(), _hashlib_st.sha256).hexdigest()
        return _requests_st.utils.quote(_requests_st.utils.quote(sig))
    
    def get_darkex_trading_history(self, start_time: _dt_st, end_time: _dt_st) -> _pd_st.DataFrame:
        """Darkex işlem geçmişini çek"""
        try:
            if not self.DARKEX_API_KEY:
                st_logger.warning("Darkex config not set")
                return _pd_st.DataFrame()
                
            path = "/fapi/v1/myTrades"
            
            # Timestamp'leri milisaniyeye çevir
            start_ts = int(start_time.timestamp() * 1000)
            end_ts = int(end_time.timestamp() * 1000)
            
            # Query parameters
            params = {
                "startTime": start_ts,
                "endTime": end_ts,
                "limit": 1000  # Maksimum limit
            }
            
            # Query string oluştur
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_path = f"{path}?{query_string}"
            
            ts = str(int(_time_st.time() * 1000))
            sig = self.generate_signature(ts, "GET", full_path, "", self.DARKEX_SECRET_KEY)
            
            headers = {
                "X-CH-APIKEY": self.DARKEX_API_KEY,
                "X-CH-TS": ts,
                "Content-Type": "application/json",
                "X-CH-SIGN": sig
            }
            
            r = _requests_st.get(self.DARKEX_BASE_URL + full_path, headers=headers, timeout=10)
            r.raise_for_status()
            
            data = r.json()
            trades = data if isinstance(data, list) else data.get("data", [])
            
            # DataFrame'e çevir
            if not trades:
                return _pd_st.DataFrame()
            
            df = _pd_st.DataFrame(trades)
            
            # Gerekli kolonları ekle/düzenle
            if not df.empty:
                df['timestamp'] = _pd_st.to_datetime(df['time'], unit='ms')
                df['realizedPnl'] = df.get('realizedPnl', 0).astype(float)
                df['qty'] = df.get('qty', 0).astype(float)
                df['price'] = df.get('price', 0).astype(float)
                df['quoteQty'] = df.get('quoteQty', 0).astype(float)
                df['commission'] = df.get('commission', 0).astype(float)
                df['side'] = df.get('side', '')
                df['symbol'] = df.get('symbol', '')
                
                # Net P&L hesapla (realized P&L - commission)
                df['netPnl'] = df['realizedPnl'] - df['commission']
            
            st_logger.info(f"Darkex history: {len(df)} trades from {start_time} to {end_time}")
            return df
            
        except Exception as e:
            st_logger.error(f"Darkex history error: {e}")
            return _pd_st.DataFrame()
    
    def get_mt5_trading_history(self, start_time: _dt_st, end_time: _dt_st) -> _pd_st.DataFrame:
        """MT5 işlem geçmişini çek"""
        try:
            # MT5 bağlantı kontrolü
            if not _mt5_st.initialize():
                st_logger.error("MT5 not initialized")
                return _pd_st.DataFrame()
            
            # Deals (işlem geçmişi) al
            deals = _mt5_st.history_deals_get(start_time, end_time)
            
            if not deals:
                st_logger.info("No MT5 deals found")
                return _pd_st.DataFrame()
            
            # DataFrame'e çevir
            deals_list = []
            for deal in deals:
                deals_list.append({
                    'ticket': deal.ticket,
                    'order': deal.order,
                    'time': _dt_st.fromtimestamp(deal.time),
                    'time_msc': deal.time_msc,
                    'type': 'BUY' if deal.type == 0 else 'SELL',
                    'entry': deal.entry,  # 0=IN, 1=OUT
                    'magic': deal.magic,
                    'position_id': deal.position_id,
                    'volume': deal.volume,
                    'price': deal.price,
                    'commission': deal.commission,
                    'swap': deal.swap,
                    'profit': deal.profit,
                    'symbol': deal.symbol,
                    'comment': deal.comment,
                    'external_id': getattr(deal, 'external_id', '')
                })
            
            df = _pd_st.DataFrame(deals_list)
            
            if not df.empty:
                # Net P&L hesapla
                df['netPnl'] = df['profit'] + df['commission'] + df['swap']
                
                # Sadece bizim magic number'ımız
                df = df[df['magic'] == 123456]
            
            st_logger.info(f"MT5 history: {len(df)} deals from {start_time} to {end_time}")
            return df
            
        except Exception as e:
            st_logger.error(f"MT5 history error: {e}")
            return _pd_st.DataFrame()
    
    def calculate_statistics(self, period_days: int = 7) -> _Dict_st:
        """İstatistikleri hesapla"""
        end_time = _dt_st.now()
        start_time = end_time - _td_st(days=period_days)
        
        # Verileri çek
        darkex_history = self.get_darkex_trading_history(start_time, end_time)
        mt5_history = self.get_mt5_trading_history(start_time, end_time)
        
        stats = {
            'period_days': period_days,
            'start_date': start_time.strftime('%Y-%m-%d'),
            'end_date': end_time.strftime('%Y-%m-%d'),
            'darkex': self._analyze_trades(darkex_history, 'Darkex'),
            'mt5': self._analyze_trades(mt5_history, 'MT5'),
            'last_update': _dt_st.now()
        }
        
        # Kombinasyon istatistikleri
        stats['combined'] = self._combine_stats(stats['darkex'], stats['mt5'])
        
        return stats
    
    def _analyze_trades(self, df: _pd_st.DataFrame, platform: str) -> _Dict_st:
        """Tek platform için trade analizi"""
        if df.empty:
            return {
                'platform': platform,
                'total_trades': 0,
                'total_pnl': 0.0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'total_volume': 0.0,
                'total_commission': 0.0,
                'largest_win': 0.0,
                'largest_loss': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'profit_factor': 0.0,
                'daily_breakdown': []
            }
        
        # Temel istatistikler
        total_trades = len(df)
        total_pnl = df['netPnl'].sum()
        
        # Kazanan/Kaybeden işlemler
        winning_trades = len(df[df['netPnl'] > 0])
        losing_trades = len(df[df['netPnl'] < 0])
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        # Volume ve commission
        if platform == 'Darkex':
            total_volume = df['quoteQty'].sum() if 'quoteQty' in df.columns else 0
            total_commission = df['commission'].sum() if 'commission' in df.columns else 0
        else:  # MT5
            total_volume = (df['volume'] * df['price']).sum()
            total_commission = df['commission'].sum()
        
        # En büyük kazanç/kayıp
        largest_win = df['netPnl'].max() if not df.empty else 0
        largest_loss = df['netPnl'].min() if not df.empty else 0
        
        # Ortalama kazanç/kayıp
        wins = df[df['netPnl'] > 0]['netPnl']
        losses = df[df['netPnl'] < 0]['netPnl']
        avg_win = wins.mean() if not wins.empty else 0
        avg_loss = losses.mean() if not losses.empty else 0
        
        # Profit factor
        gross_profit = wins.sum() if not wins.empty else 0
        gross_loss = abs(losses.sum()) if not losses.empty else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        # Günlük breakdown
        daily_breakdown = self._get_daily_breakdown(df)
        
        return {
            'platform': platform,
            'total_trades': total_trades,
            'total_pnl': float(total_pnl),
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': float(win_rate),
            'total_volume': float(total_volume),
            'total_commission': float(total_commission),
            'largest_win': float(largest_win),
            'largest_loss': float(largest_loss),
            'avg_win': float(avg_win),
            'avg_loss': float(avg_loss),
            'profit_factor': float(profit_factor),
            'daily_breakdown': daily_breakdown
        }
    
    def _get_daily_breakdown(self, df: _pd_st.DataFrame) -> _List_st[_Dict_st]:
        """Günlük breakdown"""
        if df.empty:
            return []
        
        # Timestamp kolonu kontrol et
        time_col = 'timestamp' if 'timestamp' in df.columns else 'time'
        if time_col not in df.columns:
            return []
        
        daily_stats = []
        df_copy = df.copy()
        df_copy['date'] = _pd_st.to_datetime(df_copy[time_col]).dt.date if df_copy[time_col].dtype != 'datetime64[ns]' else df_copy[time_col].dt.date
        
        for date, group in df_copy.groupby('date'):
            daily_stats.append({
                'date': date.strftime('%Y-%m-%d'),
                'trades': len(group),
                'pnl': float(group['netPnl'].sum()),
                'volume': float(group['quoteQty'].sum()) if 'quoteQty' in group.columns else float((group['volume'] * group['price']).sum())
            })
        
        return sorted(daily_stats, key=lambda x: x['date'])
    
    def _combine_stats(self, darkex_stats: _Dict_st, mt5_stats: _Dict_st) -> _Dict_st:
        """Darkex ve MT5 istatistiklerini birleştir"""
        total_trades = darkex_stats['total_trades'] + mt5_stats['total_trades']
        return {
            'total_trades': total_trades,
            'total_pnl': darkex_stats['total_pnl'] + mt5_stats['total_pnl'],
            'winning_trades': darkex_stats['winning_trades'] + mt5_stats['winning_trades'],
            'losing_trades': darkex_stats['losing_trades'] + mt5_stats['losing_trades'],
            'win_rate': ((darkex_stats['winning_trades'] + mt5_stats['winning_trades']) / 
                        max(total_trades, 1) * 100),
            'total_volume': darkex_stats['total_volume'] + mt5_stats['total_volume'],
            'total_commission': darkex_stats['total_commission'] + mt5_stats['total_commission']
        }
    
    def format_statistics_report(self, stats: _Dict_st, compact: bool = False) -> str:
        """İstatistik raporunu formatla"""
        period_text = f"{stats['period_days']} GÜN" if stats['period_days'] < 30 else f"{stats['period_days']//30} AY"
        
        if compact:
            return self._format_compact_report(stats, period_text)
        else:
            return self._format_detailed_report(stats, period_text)
    
    def _format_compact_report(self, stats: _Dict_st, period_text: str) -> str:
        """Kompakt rapor formatı"""
        dx = stats['darkex']
        mt5 = stats['mt5']
        total = stats['combined']
        
        # P&L formatla
        dx_pnl = f"{dx['total_pnl']:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        mt5_pnl = f"{mt5['total_pnl']:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        total_pnl = f"{total['total_pnl']:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        pnl_icon = "🟢" if total['total_pnl'] >= 0 else "🔴"
        
        return (
            f"📊 *İSTATİSTİK ÖZET* 📊\n\n"
            f"🗓️ *SON {period_text} RAPORU*\n\n"
            f"💰 *TOPLAM P&L:* {pnl_icon}\n"
            f"├─ Darkex: `{dx_pnl} USDT`\n"
            f"├─ MT5: `{mt5_pnl} USD`\n"
            f"└─ Toplam: `{total_pnl} USDT`\n\n"
            f"📈 *İŞLEM SAYISI:*\n"
            f"├─ Darkex: `{dx['total_trades']}`\n"
            f"├─ MT5: `{mt5['total_trades']}`\n"
            f"└─ Toplam: `{total['total_trades']}`\n\n"
            f"🎯 *BAŞARI ORANI:* `{total['win_rate']:.1f}%`\n"
            f"({total['winning_trades']} kazanan / {total['losing_trades']} kaybeden)"
        )
    
    def _format_detailed_report(self, stats: _Dict_st, period_text: str) -> str:
        """Detaylı rapor formatı"""
        dx = stats['darkex']
        mt5 = stats['mt5']
        total = stats['combined']
        
        # P&L formatla
        dx_pnl = f"{dx['total_pnl']:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        mt5_pnl = f"{mt5['total_pnl']:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        total_pnl = f"{total['total_pnl']:+,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        # Volume formatla
        dx_vol = f"{dx['total_volume']:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        mt5_vol = f"{mt5['total_volume']:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        pnl_icon = "🟢" if total['total_pnl'] >= 0 else "🔴"
        
        report = (
            f"📊 *DETAYLI İSTATİSTİK RAPORU* 📊\n\n"
            f"🗓️ *{period_text} RAPORU* ({stats['start_date']} - {stats['end_date']})\n\n"
            f"💰 *TOPLAM P&L:* {pnl_icon}\n"
            f"├─ Darkex: `{dx_pnl} USDT`\n"
            f"├─ MT5: `{mt5_pnl} USD`\n"
            f"└─ *Toplam: `{total_pnl} USDT`*\n\n"
            f"📈 *İŞLEM İSTATİSTİKLERİ:*\n"
            f"├─ Toplam İşlem: `{total['total_trades']}`\n"
            f"├─ Kazanan: `{total['winning_trades']}` ({total['win_rate']:.1f}%) 🟢\n"
            f"├─ Kaybeden: `{total['losing_trades']}` ({100-total['win_rate']:.1f}%) 🔴\n"
            f"└─ İşlem Hacmi: `{dx_vol + mt5_vol} USDT`\n\n"
        )
        
        # Platform bazlı detaylar
        if dx['total_trades'] > 0:
            report += (
                f"🟦 *DARKEX DETAY:*\n"
                f"├─ En Büyük Kazanç: `{dx['largest_win']:+.2f}`\n"
                f"├─ En Büyük Kayıp: `{dx['largest_loss']:+.2f}`\n"
                f"├─ Ort. Kazanç: `{dx['avg_win']:+.2f}`\n"
                f"├─ Ort. Kayıp: `{dx['avg_loss']:+.2f}`\n"
                f"└─ Profit Factor: `{dx['profit_factor']:.2f}`\n\n"
            )
        
        if mt5['total_trades'] > 0:
            report += (
                f"🟨 *MT5 DETAY:*\n"
                f"├─ En Büyük Kazanç: `{mt5['largest_win']:+.2f}`\n"
                f"├─ En Büyük Kayıp: `{mt5['largest_loss']:+.2f}`\n"
                f"├─ Ort. Kazanç: `{mt5['avg_win']:+.2f}`\n"
                f"├─ Ort. Kayıp: `{mt5['avg_loss']:+.2f}`\n"
                f"└─ Profit Factor: `{mt5['profit_factor']:.2f}`\n\n"
            )
        
        # Son güncelleme
        update_time = stats['last_update'].strftime('%H:%M:%S')
        report += f"🕐 *Son Güncelleme:* `{update_time}`"
        
        return report
    
    def get_daily_breakdown_chart(self, stats: _Dict_st) -> str:
        """Günlük breakdown tablosu"""
        dx_daily = stats['darkex']['daily_breakdown']
        mt5_daily = stats['mt5']['daily_breakdown']
        
        # Tarihleri birleştir
        all_dates = set()
        dx_dict = {item['date']: item for item in dx_daily}
        mt5_dict = {item['date']: item for item in mt5_daily}
        all_dates.update(dx_dict.keys())
        all_dates.update(mt5_dict.keys())
        
        if not all_dates:
            return "📅 *Günlük detay bulunamadı.*"
        
        chart = "📅 *GÜNLÜK P&L BREAKDOWN:*\n\n"
        chart += f"{'Tarih':<12} {'Darkex':<10} {'MT5':<10} {'Toplam':<10}\n"
        chart += "─" * 45 + "\n"
        
        for date in sorted(all_dates):
            dx_data = dx_dict.get(date, {'pnl': 0, 'trades': 0})
            mt5_data = mt5_dict.get(date, {'pnl': 0, 'trades': 0})
            
            dx_pnl = dx_data['pnl']
            mt5_pnl = mt5_data['pnl']
            total_pnl = dx_pnl + mt5_pnl
            
            # Kısa tarih formatı
            short_date = date[-5:]  # MM-DD
            
            chart += f"{short_date:<12} {dx_pnl:+7.1f}  {mt5_pnl:+7.1f}  {total_pnl:+7.1f}\n"
        
        return f"```\n{chart}```"

# Global instance
trading_stats = None

def init_trading_stats(api_key, secret_key, base_url, symbol_map):
    """Trading statistics'i initialize et"""
    global trading_stats
    trading_stats = TradingStatistics()
    trading_stats.set_config(api_key, secret_key, base_url, symbol_map)
    return trading_stats

def get_statistics(period_days: int = 7) -> _Dict_st:
    """İstatistikleri al"""
    global trading_stats
    if trading_stats is None:
        return {
            'error': 'Trading statistics not initialized',
            'period_days': period_days,
            'start_date': '',
            'end_date': '',
            'darkex': {'platform': 'Darkex', 'total_trades': 0, 'total_pnl': 0.0, 'winning_trades': 0, 'losing_trades': 0, 'win_rate': 0.0, 'total_volume': 0.0, 'total_commission': 0.0, 'largest_win': 0.0, 'largest_loss': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'profit_factor': 0.0, 'daily_breakdown': []},
            'mt5': {'platform': 'MT5', 'total_trades': 0, 'total_pnl': 0.0, 'winning_trades': 0, 'losing_trades': 0, 'win_rate': 0.0, 'total_volume': 0.0, 'total_commission': 0.0, 'largest_win': 0.0, 'largest_loss': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'profit_factor': 0.0, 'daily_breakdown': []},
            'combined': {'total_trades': 0, 'total_pnl': 0.0, 'winning_trades': 0, 'losing_trades': 0, 'win_rate': 0.0, 'total_volume': 0.0, 'total_commission': 0.0},
            'last_update': _dt_st.now()
        }
    return trading_stats.calculate_statistics(period_days)

def format_stats_report(stats: _Dict_st, detailed: bool = True) -> str:
    """İstatistik raporunu formatla"""
    global trading_stats
    if trading_stats is None:
        return "❌ İstatistik modülü başlatılmamış"
    return trading_stats.format_statistics_report(stats, compact=not detailed)

# statistics modül alias
sys.modules['statistics'] = sys.modules[__name__]
# ---------------------------
# statistics.py (bitiş)
# ---------------------------


# ---------------------------
# telegram_bot.py (başlangıç)
# ---------------------------
import time as _time_tb
import logging as _logging_tb
import requests as _requests_tb
import json as _json_tb
from datetime import datetime as _dt_tb
import MetaTrader5 as _mt5_tb

from copy_trading import (
    get_open_positions_df,
    get_futures_account_balance_df,
    mt5_queue,
    bot_status,
    CONTRACT_SIZE
)
from statistics import get_statistics, format_stats_report, trading_stats

tb_logger = _logging_tb.getLogger(__name__)

# --- Configuration ---
TELEGRAM_BOT_TOKEN = "7895901821:AAEJs3mmWxiWrRyVKcRiAxMN2Rn4IpiyV0o"
CHAT_ID            = -4678220102

# === Helper Functions ===
def send_text(token, chat_id, text, parse_mode='Markdown', reply_markup=None):
    """Send a message via Telegram bot API."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = _json_tb.dumps(reply_markup)
    try:
        r = _requests_tb.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        tb_logger.error("Telegram send failed: %s", e)

def send_inline_keyboard(token, chat_id, text, buttons):
    """Send message with inline keyboard buttons."""
    keyboard = {"inline_keyboard": buttons}
    send_text(token, chat_id, text, reply_markup=keyboard)

def get_main_menu_keyboard():
    """Ana menü inline keyboard butonları"""
    return [
        [
            {"text": "💰 Bakiye", "callback_data": "balance"},
            {"text": "📊 Pozisyonlar", "callback_data": "positions"}
        ],
        [
            {"text": "📈 P&L Raporu", "callback_data": "pnl"},
            {"text": "📊 İstatistikler", "callback_data": "statistics"}
        ],
        [
            {"text": "▶️ İzlemeyi Başlat", "callback_data": "start_monitoring"},
            {"text": "🛑 İzlemeyi Durdur", "callback_data": "stop_monitoring"}
        ],
        [
            {"text": "🔄 Doğrulama", "callback_data": "validate"},
            {"text": "⚙️ Ayarlar", "callback_data": "settings"}
        ],
        [
            {"text": "🆘 Yardım", "callback_data": "help"}
        ]
    ]

def get_system_status():
    """Sistem durumu string'i döndür"""
    darkex_icon = "🟢" if bot_status['darkex_connected'] else "🔴"
    mt5_icon = "🟢" if bot_status['mt5_connected'] else "🔴"
    monitor_icon = "🟢" if bot_status['monitoring_active'] else "🟡"
    
    return (
        f"📊 *SİSTEM DURUMU:*\n"
        f"├─ Darkex API: {darkex_icon} {'Bağlı' if bot_status['darkex_connected'] else 'Bağlantı Yok'}\n"
        f"├─ MT5 Terminal: {mt5_icon} {'Bağlı' if bot_status['mt5_connected'] else 'Bağlantı Yok'}\n"
        f"└─ Monitoring: {monitor_icon} {'Aktif' if bot_status['monitoring_active'] else 'Hazır'}\n"
    )

def send_main_menu(chat_id):
    """Ana menüyü gönder"""
    system_status = get_system_status()
    uptime = _dt_tb.now() - bot_status.get('start_time', _dt_tb.now())
    uptime_str = str(uptime).split('.')[0]
    
    welcome_text = (
        f"🚀 *DARKEX HEDGECONFIG BOT* 🚀\n\n"
        f"Darkex ve MT5 arasında otomatik pozisyon\n"
        f"senkronizasyonu ve risk yönetimi sağlar.\n\n"
        f"{system_status}\n"
        f"⏰ Çalışma Süresi: `{uptime_str}`\n\n"
        f"Aşağıdaki menüden istediğiniz işlemi seçin:"
    )
    
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, chat_id, welcome_text, get_main_menu_keyboard())

# alias for copy_trading notifications
def send_text_to_telegram(token, chat_id, text):
    send_text(token, chat_id, text)

def get_updates(token, offset=None, timeout=30):
    """Long-poll for Telegram updates."""
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params = {"timeout": timeout}
    if offset:
        params["offset"] = offset
    try:
        r = _requests_tb.get(url, params=params, timeout=timeout+5)
        r.raise_for_status()
        return r.json().get("result", [])
    except Exception as e:
        tb_logger.error("get_updates error: %s", e)
        return []

def answer_callback_query(callback_query_id, text=None):
    """Callback query'yi yanıtla"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    try:
        _requests_tb.post(url, data=payload, timeout=5)
    except Exception as e:
        tb_logger.error("Answer callback query failed: %s", e)

def set_bot_commands():
    """Register slash commands in Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
    commands = [
        {"command":"menu",             "description":"Ana menü"},
        {"command":"statistics",       "description":"İstatistik dashboard"},
        {"command":"balance",          "description":"Bakiye sorgula"},
        {"command":"positions",        "description":"Pozisyonları listele"},
        {"command":"pnl",              "description":"Kar/Zarar raporu"},
        {"command":"summary",          "description":"Genel özet"},
        {"command":"status",           "description":"Bot durumu"},
        {"command":"uptime",           "description":"Çalışma süresi"},
        {"command":"start_monitoring", "description":"Takibi başlat"},
        {"command":"stop_monitoring",  "description":"Takibi durdur"},
        {"command":"emergency_stop",   "description":"Acil durdur"},
        {"command":"validate",         "description":"Senkronizasyon kontrolü"},
        {"command":"compare_all",      "description":"Detaylı karşılaştırma"},
        {"command":"help",             "description":"Yardım"},
    ]
    try:
        resp = _requests_tb.post(url, json={"commands": commands}, timeout=5)
        resp.raise_for_status()
        tb_logger.info("Bot commands registered")
    except Exception as e:
        tb_logger.error("Failed to set bot commands: %s", e)

# === Command Handlers ===
def handle_menu(update):
    """Ana menüyü göster"""
    cid = update["message"]["chat"]["id"]
    send_main_menu(cid)

def handle_help(update):
    """Yardım menüsü"""
    cid = update["message"]["chat"]["id"]
    
    help_text = (
        f"🆘 *DARKEX HEDGECONFIG BOT YARDIM* 🆘\n\n"
        f"*📊 Temel Komutlar:*\n"
        f"• `/menu` - Ana menüyü göster\n"
        f"• `/statistics` - İstatistik dashboard\n"
        f"• `/balance` - Hesap bakiyelerini göster\n"
        f"• `/positions` - Açık pozisyonları listele\n"
        f"• `/pnl` - Kar/Zarar raporu\n"
        f"• `/summary` - Genel özet (bakiye + pozisyonlar)\n\n"
        f"*🔧 Kontrol Komutları:*\n"
        f"• `/start_monitoring` - Pozisyon takibini başlat\n"
        f"• `/stop_monitoring` - Pozisyon takibini durdur\n"
        f"• `/emergency_stop` - Acil durdurma\n\n"
        f"*🔍 Doğrulama Komutları:*\n"
        f"• `/validate` - Senkronizasyon kontrolü\n"
        f"• `/compare_all` - Detaylı pozisyon karşılaştırması\n\n"
        f"*ℹ️ Bilgi Komutları:*\n"
        f"• `/status` - Bot durumu\n"
        f"• `/uptime` - Çalışma süresi\n\n"
        f"*📈 İstatistik Özellikleri:*\n"
        f"• Günlük/Haftalık/Aylık kar/zarar analizi\n"
        f"• Kazanma oranı ve işlem istatistikleri\n"
        f"• Platform bazlı detaylı raporlar\n"
        f"• Günlük breakdown tabloları\n\n"
        f"*🚀 Bot Hakkında:*\n"
        f"Bu bot Darkex ve MT5 arasında otomatik pozisyon\n"
        f"senkronizasyonu sağlar. Darkex'teki pozisyon\n"
        f"değişikliklerini takip eder ve MT5'te eşzamanlı\n"
        f"işlem gerçekleştirir."
    )
    
    back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, help_text, back_button)

def get_statistics_menu_keyboard():
    """İstatistik menüsü butonları"""
    return [
        [
            {"text": "📊 1 Gün", "callback_data": "stats_1d"},
            {"text": "📊 7 Gün", "callback_data": "stats_7d"}
        ],
        [
            {"text": "📊 30 Gün", "callback_data": "stats_30d"},
            {"text": "📊 90 Gün", "callback_data": "stats_90d"}
        ],
        [
            {"text": "📈 Günlük Detay", "callback_data": "stats_daily"},
            {"text": "🔄 Yenile", "callback_data": "stats_refresh"}
        ],
        [
            {"text": "🔙 Ana Menü", "callback_data": "main_menu"}
        ]
    ]

def handle_statistics_menu(update):
    """İstatistik ana menüsü"""
    if "callback_query" in update:
        cid = update["callback_query"]["message"]["chat"]["id"]
        answer_callback_query(update["callback_query"]["id"])
    else:
        cid = update["message"]["chat"]["id"]
    
    menu_text = (
        f"📊 *İSTATİSTİK DASHBOARD* 📊\n\n"
        f"Darkex ve MT5 işlem geçmişinizi analiz edin.\n"
        f"Kar/zarar raporları ve performance metrikleri.\n\n"
        f"📅 *Hangi dönemi incelemek istiyorsunuz?*"
    )
    
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, menu_text, get_statistics_menu_keyboard())

def handle_statistics_period(update, period_days: int):
    """Belirtilen dönem için istatistik göster"""
    cid = update["callback_query"]["message"]["chat"]["id"]
    callback_id = update["callback_query"]["id"]
    
    # Loading mesajı
    answer_callback_query(callback_id, f"📊 {period_days} günlük rapor hazırlanıyor...")
    
    try:
        # İstatistikleri hesapla
        stats = get_statistics(period_days)
        
        # Raporu formatla
        report = format_stats_report(stats, detailed=True)
        
        # Geri dönüş butonları
        back_buttons = [
            [
                {"text": "📊 Başka Dönem", "callback_data": "statistics"},
                {"text": "🔄 Yenile", "callback_data": f"stats_{period_days}d"}
            ],
            [
                {"text": "🔙 Ana Menü", "callback_data": "main_menu"}
            ]
        ]
        
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, report, back_buttons)
        
    except Exception as e:
        tb_logger.error(f"Statistics error: {e}")
        error_text = (
            f"❌ *İstatistik Hatası*\n\n"
            f"Rapor hazırlanırken hata oluştu:\n`{str(e)}`\n\n"
            f"Lütfen daha sonra tekrar deneyin."
        )
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, error_text, back_button)

def handle_daily_breakdown(update):
    """Günlük detay tablosu"""
    cid = update["callback_query"]["message"]["chat"]["id"]
    callback_id = update["callback_query"]["id"]
    
    answer_callback_query(callback_id, "📅 Günlük detay hazırlanıyor...")
    
    try:
        # Son 30 günün istatistiklerini al
        stats = get_statistics(30)
        
        # Günlük breakdown tablosu
        daily_chart = trading_stats.get_daily_breakdown_chart(stats)
        
        report = (
            f"📅 *SON 30 GÜN GÜNLÜK DETAY* 📅\n\n"
            f"{daily_chart}\n\n"
            f"💡 *Not:* Pozitif değerler kazanç, negatif değerler kayıp gösterir."
        )
        
        back_buttons = [
            [
                {"text": "📊 Genel İstatistikler", "callback_data": "statistics"},
                {"text": "🔄 Yenile", "callback_data": "stats_daily"}
            ],
            [
                {"text": "🔙 Ana Menü", "callback_data": "main_menu"}
            ]
        ]
        
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, report, back_buttons)
        
    except Exception as e:
        tb_logger.error(f"Daily breakdown error: {e}")
        error_text = f"❌ Günlük detay hazırlanamadı: {str(e)}"
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, error_text, back_button)

def handle_settings(update):
    """Ayarlar menüsü (callback veya komut)"""
    if "callback_query" in update:
        cid = update["callback_query"]["message"]["chat"]["id"]
        answer_callback_query(update["callback_query"]["id"])
    else:
        cid = update["message"]["chat"]["id"]
    
    settings_text = (
        f"⚙️ *BOT AYARLARI* ⚙️\n\n"
        f"*🔧 Mevcut Ayarlar:*\n"
        f"• Polling Interval: `10 saniye`\n"
        f"• MT5 Leverage: `1:1`\n"
        f"• Magic Number: `123456`\n"
        f"• Auto Restart: `Aktif`\n\n"
        f"*📊 İstatistikler:*\n"
        f"• Queue Size: `{mt5_queue.qsize()}`\n"
        f"• Son Güncelleme: `{bot_status.get('last_update', 'Hiç').strftime('%H:%M:%S') if bot_status.get('last_update') else 'Hiç'}`\n"
        f"• Monitoring: `{'Aktif' if bot_status['monitoring_active'] else 'Pasif'}`\n\n"
        f"*🔗 Bağlantı Durumu:*\n"
        f"• Darkex API: `{'🟢 Bağlı' if bot_status['darkex_connected'] else '🔴 Bağlantı Yok'}`\n"
        f"• MT5 Terminal: `{'🟢 Bağlı' if bot_status['mt5_connected'] else '🔴 Bağlantı Yok'}`"
    )
    
    settings_buttons = [
        [
            {"text": "🔄 Ayarları Yenile", "callback_data": "refresh_settings"},
            {"text": "📊 İstatistikler", "callback_data": "statistics"}
        ],
        [
            {"text": "🔙 Ana Menü", "callback_data": "main_menu"}
        ]
    ]
    
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, settings_text, settings_buttons)
    """Ayarlar menüsü (callback veya komut)"""
    if "callback_query" in update:
        cid = update["callback_query"]["message"]["chat"]["id"]
    else:
        cid = update["message"]["chat"]["id"]
    
    settings_text = (
        f"⚙️ *BOT AYARLARI* ⚙️\n\n"
        f"*🔧 Mevcut Ayarlar:*\n"
        f"• Polling Interval: `10 saniye`\n"
        f"• MT5 Leverage: `1:1`\n"
        f"• Magic Number: `123456`\n"
        f"• Auto Restart: `Aktif`\n\n"
        f"*📊 İstatistikler:*\n"
        f"• Toplam İşlem: `{mt5_queue.qsize()}`\n"
        f"• Son Güncelleme: `{bot_status.get('last_update', 'Hiç').strftime('%H:%M:%S') if bot_status.get('last_update') else 'Hiç'}`\n"
    )
    
    settings_buttons = [
        [{"text": "🔄 Ayarları Yenile", "callback_data": "refresh_settings"}],
        [{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]
    ]
    
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, settings_text, settings_buttons)

def handle_balance(update):
    cid = update["message"]["chat"]["id"]
    # Darkex balance
    try:
        df = get_futures_account_balance_df()
        dark_bal = df["totalEquity"].iloc[0] if not df.empty else None
    except Exception:
        dark_bal = None
    # MT5 balance & equity
    try:
        acc        = _mt5_tb.account_info()
        mt5_bal    = acc.balance if acc else None
        mt5_equity = acc.equity  if acc else None
    except Exception:
        mt5_bal    = None
        mt5_equity = None

    lines = []
    if dark_bal is not None:
        dark_bal_formatted = f"{dark_bal:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        lines.append(f"💰 *Darkex Balance:* `{dark_bal_formatted} USDT`")
    else:
        lines.append("💰 *Darkex Balance:* ❌")
        
    if mt5_bal is not None:
        mt5_bal_formatted = f"{mt5_bal:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        lines.append(f"🏦 *MT5 Balance:*  `{mt5_bal_formatted} USDT`")
    else:
        lines.append("🏦 *MT5 Balance:*  ❌")
        
    if mt5_equity is not None:
        mt5_equity_formatted = f"{mt5_equity:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        lines.append(f"📊 *MT5 Equity:*  `{mt5_equity_formatted} USDT`")
    else:
        lines.append("📊 *MT5 Equity:*  ❌")
    
    # Ana menüye dönüş butonu
    back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, "\n".join(lines), back_button)

def handle_positions(update):
    cid = update["message"]["chat"]["id"]
    
    # Darkex pozisyonları
    dx = ["📈 *DARKEX POSITIONS:*"]
    try:
        df = get_open_positions_df()
        if df.empty:
            dx = ["📭 *Darkex: No open positions*"]
        else:
            for _, r in df.iterrows():
                emo = "🟢" if r["uPnL"] > 0 else "🔴" if r["uPnL"] < 0 else "⚪"
                # Margin hesaplama (position value / leverage)
                margin = (r['volume'] * r['openPrice'] * CONTRACT_SIZE.get(r['contractName'], 0.001)) / r.get('leverageLevel', 20)
                # Margin'i Türkiye formatında göster
                margin_formatted = f"{margin:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                # PnL formatla
                pnl_formatted = f"{r['uPnL']:+.2f}".replace(".", ",")
                
                dx.append(
                    f"{emo} `{r['contractName']}` | `{r['side']}`\n"
                    f"├─ Vol: `{r['volume']:.4f}` | Entry: `{r['openPrice']:.2f}`\n"
                    f"├─ PnL: `{pnl_formatted}` | Margin: `{margin_formatted} USDT`\n"
                    f"└─ Leverage: `{r.get('leverageLevel', 0)}x`"
                )
    except Exception as e:
        tb_logger.error("Darkex positions error: %s", e)
        dx = ["❌ *Error fetching Darkex positions*"]

    # MT5 pozisyonları
    m5 = ["\n📉 *MT5 POSITIONS:*"]
    try:
        pos = _mt5_tb.positions_get() or []
        if not pos:
            m5 = ["\n📭 *MT5: No open positions*"]
        else:
            for p in pos:
                emo  = "🟢" if p.profit > 0 else "🔴" if p.profit < 0 else "⚪"
                side = "BUY" if p.type == 0 else "SELL"
                
                # Symbol info for spread
                symbol_info = _mt5_tb.symbol_info(p.symbol)
                spread = symbol_info.spread if symbol_info else 0
                
                # Margin info
                margin = p.volume * p.price_open / _mt5_tb.account_info().leverage if _mt5_tb.account_info() else 0
                
                # Margin'i Türkiye formatında göster
                margin_formatted = f"{margin:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                
                m5.append(
                    f"{emo} `{p.symbol}` | `{side}`\n"
                    f"├─ Vol: `{p.volume:.4f}` | Entry: `{p.price_open:.2f}`\n"
                    f"├─ Current: `{p.price_current:.2f}` | Spread: `{spread}`\n"
                    f"├─ PnL: `{p.profit:+.2f}` | Swap: `{p.swap:+.2f}`\n"
                    f"└─ Margin: `{margin_formatted} USD` | Ticket: `#{p.ticket}`"
                )
    except Exception as e:
        tb_logger.error("MT5 positions error: %s", e)
        m5 = ["\n❌ *Error fetching MT5 positions*"]

    # Özet bilgi ekle
    summary = ["\n📊 *SUMMARY:*"]
    try:
        dx_count = len(df) if 'df' in locals() and not df.empty else 0
        mt5_count = len(pos) if 'pos' in locals() else 0
        
        # Total PnL hesapla
        darkex_pnl = df["uPnL"].sum() if 'df' in locals() and not df.empty else 0
        mt5_pnl = sum(p.profit for p in pos) if 'pos' in locals() else 0
        
        # Total margin hesapla
        darkex_margin = 0
        if 'df' in locals() and not df.empty:
            for _, r in df.iterrows():
                margin = (r['volume'] * r['openPrice'] * CONTRACT_SIZE.get(r['contractName'], 0.001)) / r.get('leverageLevel', 20)
                darkex_margin += margin
        
        mt5_margin = sum(p.volume * p.price_open / _mt5_tb.account_info().leverage for p in pos) if 'pos' in locals() and _mt5_tb.account_info() else 0
        
        # Margin'leri Türkiye formatında göster
        darkex_margin_formatted = f"{darkex_margin:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        mt5_margin_formatted = f"{mt5_margin:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        summary.append(f"Positions: Darkex {dx_count} | MT5 {mt5_count}")
        summary.append(f"Total PnL: Darkex `{darkex_pnl:+.2f}` | MT5 `{mt5_pnl:+.2f}`")
        summary.append(f"Total Margin: Darkex `{darkex_margin_formatted}` | MT5 `{mt5_margin_formatted}`")
        
        if dx_count == mt5_count and dx_count > 0:
            summary.append("✅ Position counts match!")
        elif dx_count != mt5_count:
            summary.append(f"⚠️ Count mismatch! Diff: {abs(dx_count - mt5_count)}")
    except Exception as e:
        tb_logger.error("Summary calculation error: %s", e)

    # Ana menüye dönüş butonu
    back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, "\n".join(dx + m5 + summary), back_button)

def handle_pnl(update):
    cid = update["message"]["chat"]["id"]
    try:
        df   = get_open_positions_df()
        dpos = df["uPnL"].sum() if not df.empty else 0
        mpos = _mt5_tb.positions_get() or []
        mpnl = sum(p.profit for p in mpos)
        total = dpos + mpnl

        # Türkiye formatında göster
        dpos_formatted = f"{dpos:+.2f}".replace(".", ",")
        mpnl_formatted = f"{mpnl:+.2f}".replace(".", ",")
        total_formatted = f"{total:+.2f}".replace(".", ",")

        text = (
            f"💎 *P&L REPORT* 💎\n\n"
            f"Darkex: `{dpos_formatted} USDT`\n"
            f"MT5:    `{mpnl_formatted} USDT`\n"
            f"━━━━━━━━━━\n"
            f"Total:  `{total_formatted} USDT`"
        )
        
        # Ana menüye dönüş butonu
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)
    except Exception as e:
        tb_logger.error("handle_pnl error: %s", e)
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, "❌ PnL error")

def handle_summary(update):
    handle_balance(update)
    handle_positions(update)

def handle_status(update):
    cid = update["message"]["chat"]["id"]
    up   = _dt_tb.now() - bot_status.get('start_time', _dt_tb.now())
    last = bot_status.get('last_update')
    text = (
        f"🤖 *BOT STATUS* 🤖\n\n"
        f"Darkex: {'🟢' if bot_status['darkex_connected'] else '🔴'}\n"
        f"MT5:    {'🟢' if bot_status['mt5_connected'] else '🔴'}\n"
        f"Monitor: {'🟢' if bot_status['monitoring_active'] else '🔴'}\n\n"
        f"Uptime: `{str(up).split('.')[0]}`\n"
        f"Last Update: `{last.strftime('%H:%M:%S') if last else 'Never'}`\n"
        f"Queue Size: `{mt5_queue.qsize()}`"
    )
    
    # Ana menüye dönüş butonu
    back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)

def handle_uptime(update):
    cid = update["message"]["chat"]["id"]
    up = _dt_tb.now() - bot_status.get('start_time', _dt_tb.now())
    text = f"⏰ Uptime: `{str(up).split('.')[0]}`"
    
    # Ana menüye dönüş butonu
    back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)

def handle_start_monitoring(update):
    bot_status['monitoring_active'] = True
    text = "▶️ *Monitoring started*"
    
    if "callback_query" in update:
        cid = update["callback_query"]["message"]["chat"]["id"]
        answer_callback_query(update["callback_query"]["id"], "İzleme başlatıldı ✅")
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)
    else:
        cid = update["message"]["chat"]["id"]
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, text)

def handle_stop_monitoring(update):
    bot_status['monitoring_active'] = False
    text = "🛑 *Monitoring stopped*"
    
    if "callback_query" in update:
        cid = update["callback_query"]["message"]["chat"]["id"]
        answer_callback_query(update["callback_query"]["id"], "İzleme durduruldu ✅")
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)
    else:
        cid = update["message"]["chat"]["id"]
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, text)

def handle_emergency_stop(update):
    bot_status['monitoring_active'] = False
    while not mt5_queue.empty():
        mt5_queue.get_nowait()
    
    text = "🚨 *Emergency stop activated*"
    
    if "callback_query" in update:
        cid = update["callback_query"]["message"]["chat"]["id"]
        answer_callback_query(update["callback_query"]["id"], "Acil durdurma aktif ⚠️")
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)
    else:
        cid = update["message"]["chat"]["id"]
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, text)

def handle_validate(update):
    """Darkex ve MT5 senkronizasyonunu doğrula"""
    cid = update["message"]["chat"]["id"]
    
    try:
        # Darkex pozisyonları
        darkex_df = get_open_positions_df()
        darkex_count = len(darkex_df)
        
        # MT5 pozisyonları
        mt5_positions = _mt5_tb.positions_get() or []
        mt5_count = len(mt5_positions)
        
        # Symbol mapping
        from copy_trading import SYMBOL_MAP
        
        # Detaylı karşılaştırma
        matched = 0
        mismatched = []
        
        for _, dx_pos in darkex_df.iterrows():
            mt5_symbol = SYMBOL_MAP.get(dx_pos['contractName'])
            if not mt5_symbol:
                mismatched.append(f"❌ {dx_pos['contractName']}: No mapping")
                continue
            
            # MT5'te karşılığını bul
            found = False
            for mt5_pos in mt5_positions:
                mt5_side = 'BUY' if mt5_pos.type == 0 else 'SELL'
                if mt5_pos.symbol == mt5_symbol and mt5_side == dx_pos['side']:
                    matched += 1
                    found = True
                    break
            
            if not found:
                mismatched.append(f"❌ {dx_pos['contractName']} {dx_pos['side']}: Not in MT5")
        
        # MT5'te olup Darkex'te olmayanlar
        for mt5_pos in mt5_positions:
            # Darkex karşılığını kontrol et
            found = False
            mt5_side = 'BUY' if mt5_pos.type == 0 else 'SELL'
            
            for darkex_sym, mt5_sym in SYMBOL_MAP.items():
                if mt5_sym == mt5_pos.symbol:
                    # Bu symbol Darkex'te var mı?
                    if not darkex_df.empty:
                        dx_match = darkex_df[(darkex_df['contractName'] == darkex_sym) & 
                                           (darkex_df['side'] == mt5_side)]
                        if not dx_match.empty:
                            found = True
                            break
            
            if not found:
                mismatched.append(f"⚠️ {mt5_pos.symbol} {mt5_side}: Only in MT5")
        
        # Sync rate
        sync_rate = (matched / max(darkex_count, 1)) * 100 if darkex_count > 0 else 0
        
        text = (
            f"🔄 *SYNC VALIDATION REPORT*\n\n"
            f"*Darkex Positions:* {darkex_count}\n"
            f"*MT5 Positions:* {mt5_count}\n"
            f"*Matched:* {matched}\n"
            f"*Sync Rate:* {sync_rate:.1f}%\n\n"
        )
        
        if mismatched:
            text += "*Issues Found:*\n" + "\n".join(mismatched[:10])
            if len(mismatched) > 10:
                text += f"\n... and {len(mismatched)-10} more"
        else:
            text += "✅ *All positions synced correctly!*"
        
        # Ana menüye dönüş butonu
        back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
        send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)
        
    except Exception as e:
        tb_logger.error("handle_validate error: %s", e)
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, f"❌ *Validation error:* {str(e)}")

def handle_compare_all(update):
    """Darkex ve MT5 pozisyonlarını detaylı karşılaştır"""
    cid = update["message"]["chat"]["id"]
    
    try:
        # Darkex pozisyonları
        darkex_df = get_open_positions_df()
        
        if darkex_df.empty:
            text = "📭 *No Darkex positions to compare*"
            back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
            send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)
            return
        
        # MT5 diagnostic ile karşılaştır
        from mt5_diagnostic import MT5Diagnostic
        diag = MT5Diagnostic()
        diag.compare_positions_detailed(darkex_df, TELEGRAM_BOT_TOKEN, cid)
        
    except Exception as e:
        tb_logger.error("handle_compare_all error: %s", e)
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, cid, f"❌ *Comparison error:* {str(e)}")

def handle_unknown(update):
    cid = update["message"]["chat"]["id"]
    cmd = update["message"]["text"].split()[0]
    text = f"❓ Unknown command `{cmd}`\n\nAna menü için /menu yazın."
    
    back_button = [[{"text": "🔙 Ana Menü", "callback_data": "main_menu"}]]
    send_inline_keyboard(TELEGRAM_BOT_TOKEN, cid, text, back_button)

# Komut ve callback handler'ları
COMMAND_HANDLERS = {
    '/menu':             handle_menu,
    '/statistics':       handle_statistics_menu,
    '/balance':          handle_balance,
    '/positions':        handle_positions,
    '/pnl':              handle_pnl,
    '/summary':          handle_summary,
    '/status':           handle_status,
    '/uptime':           handle_uptime,
    '/start_monitoring': handle_start_monitoring,
    '/stop_monitoring':  handle_stop_monitoring,
    '/emergency_stop':   handle_emergency_stop,
    '/validate':         handle_validate,
    '/compare_all':      handle_compare_all,
    '/help':             handle_help,
}

def handle_callback_query(update):
    """Callback query'leri işle"""
    callback_query = update["callback_query"]
    data = callback_query["data"]
    cid = callback_query["message"]["chat"]["id"]
    
    if data == "main_menu":
        answer_callback_query(callback_query["id"])
        send_main_menu(cid)
    elif data == "statistics":
        handle_statistics_menu(update)
    elif data.startswith("stats_"):
        if data == "stats_1d":
            handle_statistics_period(update, 1)
        elif data == "stats_7d":
            handle_statistics_period(update, 7)
        elif data == "stats_30d":
            handle_statistics_period(update, 30)
        elif data == "stats_90d":
            handle_statistics_period(update, 90)
        elif data == "stats_daily":
            handle_daily_breakdown(update)
        elif data == "stats_refresh":
            handle_statistics_menu(update)
    elif data == "balance":
        answer_callback_query(callback_query["id"])
        # Fake update objesi oluştur
        fake_update = {"message": {"chat": {"id": cid}}}
        handle_balance(fake_update)
    elif data == "positions":
        answer_callback_query(callback_query["id"])
        fake_update = {"message": {"chat": {"id": cid}}}
        handle_positions(fake_update)
    elif data == "pnl":
        answer_callback_query(callback_query["id"])
        fake_update = {"message": {"chat": {"id": cid}}}
        handle_pnl(fake_update)
    elif data == "start_monitoring":
        handle_start_monitoring(update)
    elif data == "stop_monitoring":
        handle_stop_monitoring(update)
    elif data == "validate":
        answer_callback_query(callback_query["id"])
        fake_update = {"message": {"chat": {"id": cid}}}
        handle_validate(fake_update)
    elif data == "settings":
        handle_settings(update)
    elif data == "help":
        answer_callback_query(callback_query["id"])
        fake_update = {"message": {"chat": {"id": cid}}}
        handle_help(fake_update)
    elif data == "refresh_settings":
        answer_callback_query(callback_query["id"], "Ayarlar yenilendi ✅")
        handle_settings(update)
    else:
        answer_callback_query(callback_query["id"], "Bilinmeyen işlem ❌")

def handle_commands(update):
    # Callback query kontrolü
    if "callback_query" in update:
        handle_callback_query(update)
        return
    
    msg  = update.get("message", {}) or {}
    text = msg.get("text", "") or ""
    if not text.startswith('/'):
        return
    cmd = text.split()[0].lower()
    handler = COMMAND_HANDLERS.get(cmd, handle_unknown)
    try:
        handler(update)
    except Exception as e:
        tb_logger.error("Error in handler %s: %s", cmd, e)
        handle_unknown(update)

def poll_updates():
    set_bot_commands()
    
    # Bot başlangıç mesajını gönder
    send_main_menu(CHAT_ID)

    offset = None
    tb_logger.info("Starting Telegram polling…")
    while True:
        updates = get_updates(TELEGRAM_BOT_TOKEN, offset)
        for upd in updates:
            offset = upd["update_id"] + 1
            handle_commands(upd)
        _time_tb.sleep(0.2)

# telegram_bot modül alias
sys.modules['telegram_bot'] = sys.modules[__name__]
# ---------------------------
# telegram_bot.py (bitiş)
# ---------------------------


# ---------------------------
# main.py (başlangıç ve gerçek giriş noktası)
# ---------------------------
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
    # İsteğe bağlı: statistics modülünü yapılandırmak isterseniz uncomment:
    # from copy_trading import DARKEX_API_KEY, DARKEX_SECRET_KEY, DARKEX_BASE_URL, SYMBOL_MAP
    # from statistics import init_trading_stats
    # init_trading_stats(DARKEX_API_KEY, DARKEX_SECRET_KEY, DARKEX_BASE_URL, SYMBOL_MAP)

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
# ---------------------------
# main.py (bitiş)
# ---------------------------
