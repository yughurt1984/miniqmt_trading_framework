# coding: utf-8
"""
EMA标准差策略 - 完全可配置版本
基于技术指标的量化交易策略，结合了指数移动平均线(EMA)和统计学的标准差概念
用于识别股票价格的极端偏离和趋势反转机会
"""

import time
import datetime as dt
import pandas as pd
import numpy as np
import json
import logging
from typing import List, Dict, Tuple, Optional, Union
from xtquant import xtdata
from xtquant.xttype import StockAccount
from xtquant import xtconstant

# 在文件开头添加默认策略配置
DEFAULT_STRATEGY_PARAMS = {
    "short_term": 5,
    "long_term": 21,
    "std_term": 21,
    "std_times": 3,
    "order_amount": 50000,
    "sell_ratio": 0.5,
    "history_start": "20180101",
    "price_type": xtconstant.FIX_PRICE,
    "use_latest_price": True,
    "max_signal_triggers": 1,  # 每种信号类型每只股票每天最大触发次数
    "check_trading_time": True
}


class EMAIndicator:
    """EMA指标计算类"""
    
    def __init__(self, period: str = '1d'):
        """
        初始化EMA指标计算器
        
        Args:
            period: K线周期，默认为'1d'（日线）
        """
        self.period = period
        self.logger = logging.getLogger('EMAIndicator')
    
    def get_close_df(self, codes: List[str], start: str) -> pd.DataFrame:
        """
        获取指定股票代码的历史收盘价数据
        
        Args:
            codes: 股票代码列表
            start: 起始日期，格式为'YYYYMMDD'
            
        Returns:
            包含收盘价数据的DataFrame
        """
        try:
            data = xtdata.get_market_data(['close'], codes, period=self.period, start_time=start, count=-1)
            close_df: pd.DataFrame = data['close']
            close_df = close_df.reindex(index=codes).dropna(how='all', axis=1)
            return close_df
        except Exception as e:
            self.logger.error(f"获取收盘价数据失败: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def ema(series: pd.Series, span: int) -> pd.Series:
        """
        计算指数移动平均线(EMA)
        
        Args:
            series: 价格序列
            span: EMA周期
            
        Returns:
            EMA序列
        """
        return series.ewm(span=span, adjust=False).mean()
    
    def compute_temp_ema_and_std(self, close_series: pd.Series, last_price: float, 
                               short_term: int, long_term: int, std_term: int) -> Tuple[float, float, float, float, float, float]:
        """
        计算临时EMA和标准差
        
        Args:
            close_series: 历史收盘价序列
            last_price: 最新价格（用于替换最后一个收盘价）
            short_term: 短期EMA周期
            long_term: 长期EMA周期
            std_term: 标准差计算周期
            
        Returns:
            元组: (今日短期EMA, 今日长期EMA, 今日差值, 标准差, 昨日短期EMA, 昨日长期EMA)
        """
        ser = close_series.copy()
        if len(ser) < max(long_term, std_term) + 2:
            raise ValueError('历史数据不足以计算EMA和STD')

        # 用最新价格替换最后一个收盘价，形成"临时"今日K线
        ser.iloc[-1] = last_price
        
        # 计算短期和长期EMA
        ema_short = self.ema(ser, short_term)
        ema_long = self.ema(ser, long_term)
        
        # 计算EMA差值及其标准差
        diff = ema_short - ema_long
        diff_std = diff.rolling(std_term, min_periods=std_term).std()

        # 获取今日和昨日的EMA值
        ema_short_today = float(ema_short.iloc[-1])
        ema_long_today = float(ema_long.iloc[-1])
        diff_today = float(diff.iloc[-1])
        std_today = float(diff_std.iloc[-1]) if not np.isnan(diff_std.iloc[-1]) else np.nan
        ema_short_yesterday = float(ema_short.iloc[-2])
        ema_long_yesterday = float(ema_long.iloc[-2])

        return ema_short_today, ema_long_today, diff_today, std_today, ema_short_yesterday, ema_long_yesterday


class EMAStdStrategy:
    """EMA标准差策略类"""
    
    def __init__(self):
        """
        初始化EMA标准差策略，使用默认参数
        """
        # 设置日志
        self.logger = logging.getLogger('EMAStdStrategy')
        
        # 信号触发次数记录（替代原来的时间冷却）
        self.signal_counts = {
            'extreme_buy': {},      # 极端差值买入
            'trend_buy': {},        # 趋势反转买入
            'partial_sell': {},     # 部分止盈
            'full_sell': {}         # 全部清仓
        }
        
        # 直接使用默认参数
        self.max_signal_triggers = DEFAULT_STRATEGY_PARAMS['max_signal_triggers']
        self.short_term = DEFAULT_STRATEGY_PARAMS['short_term']
        self.long_term = DEFAULT_STRATEGY_PARAMS['long_term']
        self.std_term = DEFAULT_STRATEGY_PARAMS['std_term']
        self.std_times = DEFAULT_STRATEGY_PARAMS['std_times']
        self.order_amount = DEFAULT_STRATEGY_PARAMS['order_amount']
        self.sell_ratio = DEFAULT_STRATEGY_PARAMS['sell_ratio']
        self.history_start = DEFAULT_STRATEGY_PARAMS['history_start']
        self.signal_cooldown_sec = DEFAULT_STRATEGY_PARAMS.get('signal_cooldown_sec', 300)
        self.price_type = DEFAULT_STRATEGY_PARAMS['price_type']
        self.use_latest_price = DEFAULT_STRATEGY_PARAMS['use_latest_price']
        self.check_trading_time = DEFAULT_STRATEGY_PARAMS['check_trading_time']
        
        # 初始化指标计算器
        self.ema_indicator = EMAIndicator(period='1d')
        
        # 信号冷却时间（秒）
        self.last_signal_time = {}  # 记录每个股票的最后信号时间
        
        self.logger.info(f"策略初始化完成: 短周期={self.short_term}, 长周期={self.long_term}, "
                        f"标准差周期={self.std_term}, 标准差倍数={self.std_times}, "
                        f"下单金额={self.order_amount}, 卖出比例={self.sell_ratio}")
    
    def check_signal_trigger(self, symbol: str, signal_type: str) -> bool:
            """
            检查信号是否可以触发（基于次数限制）
            
            Args:
                symbol: 股票代码
                signal_type: 信号类型（extreme_buy, trend_buy, partial_sell, full_sell）
                
            Returns:
                如果可以触发返回True，否则返回False
            """
            # 每天重置计数器
            self._reset_daily_counts()
            
            # 检查该股票的该信号类型是否已达到最大触发次数
            current_count = self.signal_counts[signal_type].get(symbol, 0)
            if current_count >= self.max_signal_triggers:
                self.logger.debug(f"{symbol} 的 {signal_type} 信号已达到最大触发次数 {self.max_signal_triggers}")
                return False
            
            # 增加计数并允许触发
            self.signal_counts[signal_type][symbol] = current_count + 1
            return True
    
    def _reset_daily_counts(self):
            """每日重置信号计数"""
            today = dt.datetime.now().date()
            if not hasattr(self, 'last_reset_date') or self.last_reset_date != today:
                for signal_type in self.signal_counts:
                    self.signal_counts[signal_type] = {}
                self.last_reset_date = today
                self.logger.info("信号触发计数器已重置")    
            
    def is_trading_time(self) -> bool:
        """
        检查当前是否为交易时间
        
        Returns:
            如果是交易时间返回True，否则返回False
        """
        if not self.check_trading_time:
            return True
            
        now = dt.datetime.now()
        # 简单判断：工作日的9:30-15:00为交易时间
        if now.weekday() >= 5:  # 周末
            return False
            
        # 交易时间段判断
        if ((dt.time(9, 30) <= now.time() <= dt.time(11, 30)) or 
            (dt.time(13, 0) <= now.time() <= dt.time(15, 0))):
            return True
            
        return False
    
    
    def generate_signals(self, data: Dict) -> List[Dict]:
        """
        生成交易信号
        
        Args:
            data: 包含市场数据、账户信息、持仓的数据字典
            
        Returns:
            交易信号列表
        """
        signals = []
        
        if not self.is_trading_time():
            return signals
            
        market_data = data.get('market_data', {})
        positions = data.get('positions', {})
        watch_list = data.get('watch_list', [])
        
        if not watch_list:
            watch_list = self.config.get('watch_list', [])
        
        # 获取所有股票的收盘价数据
        close_df = self.ema_indicator.get_close_df(watch_list, self.history_start)
        if close_df.empty:
            self.logger.warning("获取收盘价数据失败")
            return signals
        
        # 对每只股票生成信号
        for symbol in watch_list:
            try:
                # 获取股票的市场数据
                symbol_data = market_data.get(symbol, {})
                if not symbol_data:
                    continue
                    
                current_price = symbol_data.get('price', 0)
                if current_price == 0:
                    continue
                
                # 获取历史收盘价数据
                close_series = close_df.loc[symbol].dropna()
                if len(close_series) < max(self.long_term, self.std_term) + 2:
                    self.logger.warning(f"{symbol} 历史数据不足")
                    continue
                
                # 计算EMA和标准差
                ema_short_today, ema_long_today, diff_today, std_today, ema_short_yesterday, ema_long_yesterday = \
                    self.ema_indicator.compute_temp_ema_and_std(
                        close_series, current_price, self.short_term, self.long_term, self.std_term
                    )
                
                # 获取当前持仓
                position = positions.get(symbol, {}).get('volume', 0)
                
                # 生成买入信号
                buy_signal = self._check_buy_signals(
                    symbol, ema_short_today, ema_long_today, diff_today, std_today,
                    ema_short_yesterday, ema_long_yesterday, current_price, position
                )
                
                if buy_signal:
                    signals.append(buy_signal)
                    continue
                
                # 生成卖出信号
                sell_signal = self._check_sell_signals(
                    symbol, ema_short_today, ema_long_today, diff_today, std_today,
                    ema_short_yesterday, ema_long_yesterday, current_price, position
                )
                
                if sell_signal:
                    signals.append(sell_signal)
                    
            except Exception as e:
                self.logger.error(f"处理股票 {symbol} 时出错: {e}")
                continue
        
        return signals
    
    def _check_buy_signals(self, symbol: str, ema_short_today: float, ema_long_today: float, 
                     diff_today: float, std_today: float, ema_short_yesterday: float, 
                     ema_long_yesterday: float, current_price: float, position: int) -> Optional[Dict]:
        """检查买入信号"""
    
    # 信号1: 极端差值买入
        if (ema_short_today < ema_long_today and 
            abs(diff_today) > self.std_times * std_today and
            self.check_signal_trigger(symbol, "extreme_buy")):
            
            quantity = int(self.order_amount / current_price / 100) * 100  # 向下取整到100的倍数
            if quantity > 0:
                self.logger.info(f"{symbol} 极端差值买入信号: 当前价格={current_price}, "
                                f"EMA短期={ema_short_today}, EMA长期={ema_long_today}, "
                                f"差值={diff_today}, 标准差={std_today}")
                return {
                    'symbol': symbol,
                    'order_type': xtconstant.STOCK_BUY,
                    'price': current_price,
                    'quantity': quantity,
                    'reason': '极端差值买入'
                }
        
        # 信号2: 趋势反转买入（金叉）
        if (ema_short_yesterday < ema_long_yesterday and 
            ema_short_today > ema_long_today and
            self.check_signal_trigger(symbol, "trend_buy")):
            
            quantity = int(self.order_amount / current_price / 100) * 100  # 向下取整到100的倍数
            if quantity > 0:
                self.logger.info(f"{symbol} 趋势反转买入信号(金叉): 当前价格={current_price}, "
                                f"昨日EMA短期={ema_short_yesterday}, 昨日EMA长期={ema_long_yesterday}, "
                                f"今日EMA短期={ema_short_today}, 今日EMA长期={ema_long_today}")
                return {
                    'symbol': symbol,
                    'order_type': xtconstant.STOCK_BUY,
                    'price': current_price,
                    'quantity': quantity,
                    'reason': '趋势反转买入'
                }
        
        return None
    
    def _check_sell_signals(self, symbol: str, ema_short_today: float, ema_long_today: float, 
                      diff_today: float, std_today: float, ema_short_yesterday: float, 
                      ema_long_yesterday: float, current_price: float, position: int) -> Optional[Dict]:
        """检查卖出信号"""
        
        # 只有有持仓时才考虑卖出
        if position <= 0:
            return None
        
        # 信号1: 部分止盈
        if (ema_short_today > ema_long_today and 
            abs(diff_today) > self.std_times * std_today and
            self.check_signal_trigger(symbol, "partial_sell")):
            
            sell_quantity = int(position * self.sell_ratio / 100) * 100  # 向下取整到100的倍数
            if sell_quantity > 0:
                self.logger.info(f"{symbol} 部分止盈信号: 当前价格={current_price}, "
                                f"EMA短期={ema_short_today}, EMA长期={ema_long_today}, "
                                f"差值={diff_today}, 标准差={std_today}, 卖出数量={sell_quantity}")
                return {
                    'symbol': symbol,
                    'order_type': xtconstant.STOCK_SELL,
                    'price': current_price,
                    'quantity': sell_quantity,
                    'reason': '部分止盈'
                }
        
        # 信号2: 全部清仓（死叉）
        if (ema_short_yesterday > ema_long_yesterday and 
            ema_short_today < ema_long_today and
            self.check_signal_trigger(symbol, "full_sell")):
            
            sell_quantity = int(position / 100) * 100  # 向下取整到100的倍数
            if sell_quantity > 0:
                self.logger.info(f"{symbol} 全部清仓信号(死叉): 当前价格={current_price}, "
                                f"昨日EMA短期={ema_short_yesterday}, 昨日EMA长期={ema_long_yesterday}, "
                                f"今日EMA短期={ema_short_today}, 今日EMA长期={ema_long_today}, "
                                f"卖出数量={sell_quantity}")
                return {
                    'symbol': symbol,
                    'order_type': xtconstant.STOCK_SELL,
                    'price': current_price,
                    'quantity': sell_quantity,
                    'reason': '全部清仓'
                }
        
        return None

