"""
风控模块
"""
from typing import Dict, List, Tuple
from datetime import datetime

DEFAULT_RISK_RULES = {
    "max_position_ratio": 0.1,
    "max_daily_trades": 10,
    "max_order_value": 50000,
    "blacklist": []
}

class RiskControl:
    """风控管理"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.daily_trades = 0
        self.today = datetime.now().date()
        
        # 使用配置中的风控规则或默认值
        self.risk_rules = config.get('risk_rules', DEFAULT_RISK_RULES)
    
    def check_signal(self, signal: Dict, account_info: Dict, positions: Dict) -> Tuple[bool, str]:
        """
        检查交易信号风险
        
        Returns:
            (是否通过, 拒绝原因)
        """
        symbol = signal['symbol']
        action = signal['action']
        price = signal['price']
        quantity = signal['quantity']
        value = price * quantity
        
        # 检查黑名单
        if symbol in self.risk_rules.get('blacklist', []):
            return False, f"{symbol} 在黑名单中"
        
        # 检查单日交易次数
        if self.daily_trades >= self.risk_rules.get('max_daily_trades', 10):
            return False, "超过单日最大交易次数"
        
        # 检查单笔订单金额
        max_order_value = self.risk_rules.get('max_order_value', 50000)
        if value > max_order_value:
            return False, f"单笔订单金额 {value} 超过限制 {max_order_value}"
        
        # 买入风控
        if action == 'buy':
            # 检查仓位比例
            position_value = positions.get(symbol, {}).get('market_value', 0)
            total_asset = account_info.get('total_asset', 1)
            max_ratio = self.risk_rules.get('max_position_ratio', 0.1)
            
            new_ratio = (position_value + value) / total_asset
            if new_ratio > max_ratio:
                return False, f"仓位比例 {new_ratio:.2%} 超过限制 {max_ratio:.2%}"
            
            # 检查资金是否充足
            available_cash = account_info.get('available_cash', 0)
            if value > available_cash:
                return False, f"资金不足: 需要 {value}, 可用 {available_cash}"
        
        # 更新交易统计
        self._update_daily_statistics()
        
        return True, "通过"
    
    def _update_daily_statistics(self):
        """更新每日统计"""
        today = datetime.now().date()
        if today != self.today:
            self.daily_trades = 0
            self.today = today
        
        self.daily_trades += 1