"""
移动平均线策略
基于快慢均线的金叉死叉生成交易信号
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from xtquant import xtdata

from logger import setup_logger


class MovingAverageStrategy:
    """移动平均线策略"""
    
    def __init__(self, config: Dict[str, Any] = None):
        # 设置日志
        self.logger = setup_logger("MovingAverageStrategy", "INFO")
        
        # 策略参数
        if config is None:
            config = {}
        self.fast_period = config.get('fast_period', 5)
        self.slow_period = config.get('slow_period', 20)
        self.volume = config.get('volume', 100)  # 默认交易量
        
        # 获取股票列表
        self.watch_list = config.get('watch_list', ["000001.SZ", "000002.SZ"])
        
        # 策略状态
        self.position_status = {}  # 记录每个股票的持仓状态
        self.last_signal = {}  # 记录每个股票的上一次信号
        
        self.logger.info(f"移动平均策略初始化: 快线{self.fast_period}日, 慢线{self.slow_period}日, 交易量{self.volume}")
    
    def generate_signals(self, data: Dict[str, Any]) -> List[Dict]:
        """
        生成交易信号
        :param data: 包含市场数据、账户信息等的字典
        :return: 信号列表，每个信号是包含order_type, quantity, price, symbol的字典
        """
        signals = []
        market_data = data.get('market_data', {})
        positions = data.get('positions', {})
        watch_list = data.get('watch_list', self.watch_list)
        
        for stock_code in watch_list:
            try:
                # 获取K线数据
                kline_data = self._get_kline_data(stock_code)
                if kline_data is None or len(kline_data) < self.slow_period:
                    continue
                
                # 计算均线
                fast_ma = kline_data['close'].rolling(window=self.fast_period).mean()
                slow_ma = kline_data['close'].rolling(window=self.slow_period).mean()
                
                # 获取当前价格
                current_price = float(kline_data['close'].iloc[-1])
                
                # 生成信号
                signal = self._generate_signal(stock_code, fast_ma, slow_ma, current_price, positions)
                if signal:
                    signals.append(signal)
                    
            except Exception as e:
                self.logger.error(f"计算{stock_code}信号失败: {e}")
        
        return signals
    
    def _get_kline_data(self, stock_code: str, period: int = 100) -> Optional[pd.DataFrame]:
        """
        获取K线数据
        :param stock_code: 股票代码
        :param period: 获取数据天数
        :return: 包含OHLCV数据的DataFrame
        """
        try:
            # 使用xtquant获取K线数据
            end_time = datetime.now().strftime('%Y%m%d')
            fields = ['open', 'high', 'low', 'close', 'volume']
            
            # 获取日线数据
            kline_data = xtdata.get_market_data(
                stock_list=[stock_code],
                period='1d',
                start_time='',
                end_time=end_time,
                count=period,
                dividend_type='front',
                fill_data=True
            )
            
            if kline_data is None or stock_code not in kline_data:
                self.logger.warning(f"获取{stock_code}K线数据失败")
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame()
            for field in fields:
                if field in kline_data[stock_code]:
                    df[field] = kline_data[stock_code][field]
            
            if df.empty:
                self.logger.warning(f"{stock_code}K线数据为空")
                return None
            
            return df
        except Exception as e:
            self.logger.error(f"获取{stock_code}K线数据失败: {e}")
            return None
    
    def _generate_signal(self, stock_code: str, fast_ma: pd.Series, slow_ma: pd.Series, 
                         current_price: float, positions: Dict) -> Optional[Dict]:
        """
        生成交易信号
        :param stock_code: 股票代码
        :param fast_ma: 快速移动平均线
        :param slow_ma: 慢速移动平均线
        :param current_price: 当前价格
        :param positions: 持仓信息
        :return: 信号字典或None
        """
        if len(fast_ma) < 2 or len(slow_ma) < 2:
            return None
        
        current_fast = fast_ma.iloc[-1]
        current_slow = slow_ma.iloc[-1]
        prev_fast = fast_ma.iloc[-2]
        prev_slow = slow_ma.iloc[-2]
        
        # 检查是否有持仓
        has_position = stock_code in positions and positions[stock_code].get('volume', 0) > 0
        
        # 避免重复信号
        last_signal = self.last_signal.get(stock_code)
        
        # 金叉买入信号
        if (prev_fast <= prev_slow) and (current_fast > current_slow) and not has_position:
            if last_signal != 'buy':
                self.logger.info(f"{stock_code} 金叉买入信号: 快线{current_fast:.2f}上穿慢线{current_slow:.2f}")
                self.last_signal[stock_code] = 'buy'
                
                return {
                    'order_type': 'buy',
                    'quantity': self.volume,
                    'price': current_price,
                    'symbol': stock_code
                }
        
        # 死叉卖出信号
        elif (prev_fast >= prev_slow) and (current_fast < current_slow) and has_position:
            if last_signal != 'sell':
                self.logger.info(f"{stock_code} 死叉卖出信号: 快线{current_fast:.2f}下穿慢线{current_slow:.2f}")
                self.last_signal[stock_code] = 'sell'
                
                # 卖出时使用实际持仓数量
                sell_volume = positions[stock_code].get('volume', self.volume)
                
                return {
                    'order_type': 'sell',
                    'quantity': sell_volume,
                    'price': current_price,
                    'symbol': stock_code
                }
        
        return None