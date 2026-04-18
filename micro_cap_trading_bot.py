#!/usr/bin/env python3
import os
"""
MICRO-CAP TRADING BOT - Under 10 Cents
Optimized for $6 budget maximum
Only trades tickers with nominal value under $0.10
"""

import asyncio
import aiohttp
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import logging
from dataclasses import dataclass, field
from enum import Enum
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderSide(Enum):
    SHORT = "sell"
    COVER = "buy"

@dataclass
class MicroTicker:
    symbol: str
    price: float
    price_24h_ago: float
    volume_24h: float
    exchange: str
    nominal_value: float
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class MicroPosition:
    symbol: str
    exchange: str
    entry_price: float
    quantity: float
    entry_time: datetime
    nominal_value: float
    status: str = "open"
    pnl: float = 0.0
    profit_taken: bool = False

class MicroCapTradingBot:
    """Specialized micro-cap trading bot for under 10 cents"""
    
    def __init__(self):
        # STRICT BUDGET AND NOMINAL VALUE CONSTRAINTS
        self.total_budget = 6.0  # $6 maximum budget
        self.max_nominal_value = 0.10  # 10 cents maximum
        self.min_nominal_value = 0.0001  # Minimum practical value
        
        # Position sizing for $6 budget
        self.max_positions = 20  # Maximum micro positions
        self.base_position_size = 0.25  # $0.25 per position (allows 24 positions)
        self.max_per_symbol = 1.0  # Maximum $1 per symbol
        
        # Micro-cap optimized parameters
        self.pump_threshold = 0.10  # 10% pump threshold for micro-caps
        self.profit_target = 0.05  # 5% profit target (micro-caps are volatile)
        self.stop_loss = 0.03  # 3% stop loss
        
        # Exchange configurations for micro-cap scanning
        self.exchanges = {
            'binance': {
                'futures_url': 'https://fapi.binance.com/fapi/v1/ticker/24hr',
                'min_notional': 1.0,  # $1 minimum on Binance
            },
            'bybit': {
                'futures_url': 'https://api.bybit.com/v5/market/tickers?category=linear&instType=USDT-FUTURES',
                'min_notional': 1.0,  # $1 minimum on Bybit
            },
            'kucoin': {
                'futures_url': 'https://api-futures.kucoin.com/api/v1/allTickers',
                'min_notional': 1.0,  # $1 minimum on KuCoin
            }
        }
        
        # State tracking
        self.active_positions = []
        self.profit_symbols_today = set()
        self.last_scan_time = datetime.now()
        self.daily_budget_used = 0.0
        
        # Statistics
        self.total_trades = 0
        self.successful_shorts = 0
        self.total_pnl = 0.0
        
        logger.info("🪙 MICRO-CAP TRADING BOT INITIALIZED")
        logger.info(f"💰 Total Budget: ${self.total_budget}")
        logger.info(f"📊 Max Nominal Value: ${self.max_nominal_value}")
        logger.info(f"🎯 Base Position Size: ${self.base_position_size}")
        logger.info(f"📈 Pump Threshold: {self.pump_threshold*100:.0f}%")
    
    async def get_binance_micro_tickers(self) -> List[MicroTicker]:
        """Get Binance futures tickers under 10 cents"""
        try:
            url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        micro_tickers = []
                        
                        for ticker in data:
                            try:
                                symbol = ticker['symbol']
                                if not symbol.endswith('USDT'):
                                    continue
                                
                                price = float(ticker['lastPrice'])
                                price_change = float(ticker['priceChangePercent']) / 100
                                
                                # STRICT FILTER: Only under 10 cents
                                if price >= self.max_nominal_value or price <= 0:
                                    continue
                                
                                # Calculate nominal value for minimum order
                                min_qty = 0.001  # Minimum quantity
                                nominal_value = min_qty * price
                                
                                # Must be under 10 cents nominal value
                                if nominal_value >= self.max_nominal_value:
                                    continue
                                
                                # Calculate 24h ago price
                                price_24h_ago = price / (1 + price_change) if price_change != 0 else price
                                
                                micro_ticker = MicroTicker(
                                    symbol=symbol.replace('USDT', ''),
                                    price=price,
                                    price_24h_ago=price_24h_ago,
                                    volume_24h=float(ticker['quoteVolume']),
                                    exchange='binance',
                                    nominal_value=nominal_value
                                )
                                micro_tickers.append(micro_ticker)
                                
                            except (ValueError, KeyError):
                                continue
                        
                        logger.info(f"📊 Binance: Found {len(micro_tickers)} micro-cap tickers under ${self.max_nominal_value}")
                        return micro_tickers
                        
        except Exception as e:
            logger.error(f"Binance API error: {e}")
        
        return []
    
    async def get_bybit_micro_tickers(self) -> List[MicroTicker]:
        """Get Bybit futures tickers under 10 cents"""
        try:
            url = "https://api.bybit.com/v5/market/tickers?category=linear&instType=USDT-FUTURES"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        micro_tickers = []
                        
                        if data['retCode'] == 0 and data['result']['list']:
                            for ticker in data['result']['list']:
                                try:
                                    symbol = ticker['symbol']
                                    if not symbol.endswith('USDT'):
                                        continue
                                    
                                    price = float(ticker['lastPrice'])
                                    price_change = float(ticker['price24hPcnt'])
                                    
                                    # STRICT FILTER: Only under 10 cents
                                    if price >= self.max_nominal_value or price <= 0:
                                        continue
                                    
                                    # Calculate nominal value
                                    min_qty = 0.001
                                    nominal_value = min_qty * price
                                    
                                    if nominal_value >= self.max_nominal_value:
                                        continue
                                    
                                    price_24h_ago = price / (1 + price_change) if price_change != 0 else price
                                    
                                    micro_ticker = MicroTicker(
                                        symbol=symbol.replace('USDT', ''),
                                        price=price,
                                        price_24h_ago=price_24h_ago,
                                        volume_24h=float(ticker['turnover24h']) / price,
                                        exchange='bybit',
                                        nominal_value=nominal_value
                                    )
                                    micro_tickers.append(micro_ticker)
                                    
                                except (ValueError, KeyError):
                                    continue
                        
                        logger.info(f"📊 Bybit: Found {len(micro_tickers)} micro-cap tickers under ${self.max_nominal_value}")
                        return micro_tickers
                        
        except Exception as e:
            logger.error(f"Bybit API error: {e}")
        
        return []
    
    async def get_kucoin_micro_tickers(self) -> List[MicroTicker]:
        """Get KuCoin futures tickers under 10 cents"""
        try:
            url = "https://api-futures.kucoin.com/api/v1/allTickers"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        micro_tickers = []
                        
                        if data['code'] == '200' and 'data' in data:
                            for ticker in data['data']:
                                try:
                                    symbol = ticker['symbol']
                                    if not symbol.endswith('USDT'):
                                        continue
                                    
                                    price = float(ticker['last'])
                                    price_change = float(ticker['changeRate'])
                                    
                                    # STRICT FILTER: Only under 10 cents
                                    if price >= self.max_nominal_value or price <= 0:
                                        continue
                                    
                                    # Calculate nominal value
                                    min_qty = 0.001
                                    nominal_value = min_qty * price
                                    
                                    if nominal_value >= self.max_nominal_value:
                                        continue
                                    
                                    price_24h_ago = price / (1 + price_change) if price_change != 0 else price
                                    
                                    micro_ticker = MicroTicker(
                                        symbol=symbol.replace('USDT', ''),
                                        price=price,
                                        price_24h_ago=price_24h_ago,
                                        volume_24h=float(ticker['turnover24h']),
                                        exchange='kucoin',
                                        nominal_value=nominal_value
                                    )
                                    micro_tickers.append(micro_ticker)
                                    
                                except (ValueError, KeyError):
                                    continue
                        
                        logger.info(f"📊 KuCoin: Found {len(micro_tickers)} micro-cap tickers under ${self.max_nominal_value}")
                        return micro_tickers
                        
        except Exception as e:
            logger.error(f"KuCoin API error: {e}")
        
        return []
    
    def identify_micro_pumps(self, tickers: List[MicroTicker]) -> List[MicroTicker]:
        """Identify micro-cap tickers that have pumped 10%+"""
        pumping_tickers = []
        
        for ticker in tickers:
            price_change = (ticker.price - ticker.price_24h_ago) / ticker.price_24h_ago
            
            if price_change >= self.pump_threshold:
                # Check if we haven't hit profit today
                symbol_exchange_key = f"{ticker.symbol}_{ticker.exchange}"
                if symbol_exchange_key not in self.profit_symbols_today:
                    pumping_tickers.append(ticker)
                    logger.info(f"🚀 MICRO PUMP: {ticker.symbol} +{price_change*100:.1f}% "
                              f"${ticker.price:.6f} | Nominal: ${ticker.nominal_value:.6f} | {ticker.exchange}")
        
        # Sort by nominal value (smallest first - best for micro budget)
        pumping_tickers.sort(key=lambda x: x.nominal_value)
        
        return pumping_tickers
    
    def should_short_micro(self, ticker: MicroTicker) -> bool:
        """Determine if we should short this micro-cap ticker"""
        # Check budget constraints
        if self.daily_budget_used >= self.total_budget:
            return False
        
        # Check if we already have a position
        for pos in self.active_positions:
            if pos.symbol == ticker.symbol and pos.exchange == ticker.exchange:
                return False
        
        # Check maximum positions
        if len(self.active_positions) >= self.max_positions:
            return False
        
        # Check if we've exceeded $1 per symbol
        symbol_investment = sum(p.nominal_value for p in self.active_positions if p.symbol == ticker.symbol)
        if symbol_investment + ticker.nominal_value > self.max_per_symbol:
            return False
        
        # Check if symbol already hit profit today
        symbol_exchange_key = f"{ticker.symbol}_{ticker.exchange}"
        if symbol_exchange_key in self.profit_symbols_today:
            return False
        
        # Prioritize smallest nominal values
        return True
    
    async def place_micro_short(self, ticker: MicroTicker) -> Optional[MicroPosition]:
        """Place a micro short order"""
        try:
            # Calculate position size based on $6 budget
            position_size = min(self.base_position_size, self.total_budget - self.daily_budget_used)
            quantity = position_size / ticker.price
            
            # Create position
            position = MicroPosition(
                symbol=ticker.symbol,
                exchange=ticker.exchange,
                entry_price=ticker.price,
                quantity=quantity,
                entry_time=datetime.now(),
                nominal_value=position_size
            )
            
            self.active_positions.append(position)
            self.daily_budget_used += position_size
            self.total_trades += 1
            
            logger.info(f"📉 MICRO SHORT: {ticker.symbol} @ ${ticker.price:.6f}")
            logger.info(f"   Exchange: {ticker.exchange} | Nominal: ${position.nominal_value:.4f}")
            logger.info(f"   Quantity: {quantity:.6f} | Budget Used: ${self.daily_budget_used:.2f}/${self.total_budget}")
            
            return position
            
        except Exception as e:
            logger.error(f"Failed to place micro short for {ticker.symbol}: {e}")
            return None
    
    async def monitor_micro_positions(self):
        """Monitor and close micro positions"""
        current_prices = await self.get_all_current_prices()
        
        for position in self.active_positions[:]:
            if position.status != "open":
                continue
            
            # Get current price
            current_price = current_prices.get(f"{position.symbol}_{position.exchange}")
            if not current_price:
                continue
            
            # Simulate price movement for demo
            change = np.random.normal(0, 0.01)  # 1% volatility for micro-caps
            current_price = position.entry_price * (1 + change)
            
            # Calculate PnL
            pnl = (position.entry_price - current_price) * position.quantity
            pnl_pct = (pnl / position.nominal_value) * 100
            position.pnl = pnl
            
            # Check for profit taking (5% target)
            if pnl_pct >= self.profit_target * 100:
                position.status = "profit_taken"
                position.profit_taken = True
                self.successful_shorts += 1
                self.total_pnl += pnl
                self.active_positions.remove(position)
                self.daily_budget_used -= position.nominal_value
                
                # Add to profit symbols list
                symbol_exchange_key = f"{position.symbol}_{position.exchange}"
                self.profit_symbols_today.add(symbol_exchange_key)
                
                logger.info(f"💰 MICRO PROFIT: {position.symbol} @ ${current_price:.6f}")
                logger.info(f"   Profit: ${pnl:.4f} ({pnl_pct:.2f}%) | Budget Released: ${position.nominal_value:.4f}")
                
            elif pnl_pct <= -self.stop_loss * 100:
                position.status = "stop_loss"
                self.total_pnl += pnl
                self.active_positions.remove(position)
                self.daily_budget_used -= position.nominal_value
                
                logger.info(f"❌ MICRO STOP: {position.symbol} @ ${current_price:.6f} (${pnl:.4f})")
    
    async def get_all_current_prices(self) -> Dict[str, float]:
        """Get current prices for all active positions"""
        prices = {}
        
        for position in self.active_positions:
            # Simulate small random price movements
            change = np.random.normal(0, 0.005)  # 0.5% standard deviation
            current_price = position.entry_price * (1 + change)
            prices[f"{position.symbol}_{position.exchange}"] = current_price
        
        return prices
    
    async def scan_micro_caps(self):
        """Main micro-cap scanning and trading loop"""
        logger.info("🔍 Scanning micro-cap universe (under 10 cents)...")
        
        # Get all micro-cap tickers from exchanges
        all_micro_tickers = []
        
        binance_tickers = await self.get_binance_micro_tickers()
        bybit_tickers = await self.get_bybit_micro_tickers()
        kucoin_tickers = await self.get_kucoin_micro_tickers()
        
        all_micro_tickers.extend(binance_tickers)
        all_micro_tickers.extend(bybit_tickers)
        all_micro_tickers.extend(kucoin_tickers)
        
        logger.info(f"📊 Total micro-cap tickers under ${self.max_nominal_value}: {len(all_micro_tickers)}")
        
        # Identify pumping micro-caps (10%+)
        pumping_micros = self.identify_micro_pumps(all_micro_tickers)
        logger.info(f"🚀 Found {len(pumping_micros)} micro-cap pumps (10%+)")
        
        # Place micro short orders (prioritize smallest nominal values)
        new_shorts = 0
        for ticker in pumping_micros:
            if self.should_short_micro(ticker):
                position = await self.place_micro_short(ticker)
                if position:
                    new_shorts += 1
                
                # Budget constraint check
                if self.daily_budget_used >= self.total_budget * 0.8:  # Stop at 80% budget usage
                    break
        
        logger.info(f"📉 Placed {new_shorts} micro short positions")
        logger.info(f"💰 Budget Used: ${self.daily_budget_used:.2f}/${self.total_budget}")
        
        # Monitor and close positions
        await self.monitor_micro_positions()
        
        # Update statistics
        self.last_scan_time = datetime.now()
    
    def print_micro_status(self):
        """Print micro-cap bot status"""
        print("\n" + "="*100)
        print("🪙 MICRO-CAP TRADING BOT STATUS")
        print("="*100)
        print(f"💰 Total Budget: ${self.total_budget}")
        print(f"📊 Max Nominal Value: ${self.max_nominal_value}")
        print(f"🎯 Pump Threshold: {self.pump_threshold*100:.0f}%+")
        print(f"💵 Base Position: ${self.base_position_size}")
        print(f"📈 Active Positions: {len(self.active_positions)}/{self.max_positions}")
        print(f"💸 Budget Used: ${self.daily_budget_used:.2f}/{self.total_budget}")
        print(f"✅ Total Trades: {self.total_trades}")
        print(f"💰 Total PnL: ${self.total_pnl:.4f}")
        print(f"🚫 Symbols Blocked Today: {len(self.profit_symbols_today)}")
        print(f"⏰ Last Scan: {self.last_scan_time.strftime('%H:%M:%S')}")
        
        if self.active_positions:
            print(f"\n📋 Active Micro Positions:")
            for pos in self.active_positions[:10]:  # Show top 10
                pnl_pct = (pos.pnl / pos.nominal_value) * 100 if pos.nominal_value > 0 else 0
                print(f"   {pos.symbol} ({pos.exchange}): ${pos.entry_price:.6f} → "
                      f"PnL: ${pos.pnl:.4f} ({pnl_pct:+.2f}%) | Nominal: ${pos.nominal_value:.4f}")
        
        print("="*100)
    
    async def run_micro_cap_bot(self):
        """Main micro-cap trading loop"""
        logger.info("🪙 Starting Micro-Cap Trading Bot")
        logger.info(f"💰 Budget: ${self.total_budget} | Max Nominal: ${self.max_nominal_value}")
        logger.info(f"🎯 Targeting micro-caps under 10 cents with 10%+ pumps")
        
        while True:
            try:
                # Scan micro-caps and trade
                await self.scan_micro_caps()
                
                # Print status every minute
                if int(time.time()) % 60 == 0:
                    self.print_micro_status()
                
                # Wait before next scan
                await asyncio.sleep(30)  # Scan every 30 seconds
                
            except KeyboardInterrupt:
                logger.info("🛑 Micro-cap bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(10)
        
        # Final status
        self.print_micro_status()

async def main():
    """Main function"""
    print("🪙 MICRO-CAP TRADING BOT - Under 10 Cents")
    print("="*100)
    print("💰 Budget: $6.00 Maximum")
    print("📊 Only trades tickers under $0.10 nominal value")
    print("🎯 Targets 10%+ pumps on micro-caps")
    print("⚡ Optimized for micro position sizing")
    print("⚠️  DEMO MODE - Simulated trading")
    print("="*100)
    
    # Initialize micro-cap bot
    micro_bot = MicroCapTradingBot()
    
    # Run the bot
    await micro_bot.run_micro_cap_bot()

if __name__ == "__main__":
    asyncio.run(main())
