"""
QMT交易核心模块
负责账户连接、数据获取和订单执行
"""
import time
import datetime
import os
from typing import Dict, List, Optional

from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant

# 导入日志模块
from logger import setup_logger


class QMTTraderCallback:
    """QMT交易回调处理"""
    def __init__(self, logger):
        self.logger = logger
        self.order_count = 0  # 订单计数器
        self.trade_count = 0  # 成交计数器
    
    def on_disconnected(self):
        """连接断开回调"""
        self.logger.warning("QMT连接断开")
    
    def get_order_status_text(self, status_code):
        """
        委托状态码转换为文本
        """
        status_dict = {
            xtconstant.ORDER_UNREPORTED: "未报",          # 48
            xtconstant.ORDER_WAIT_REPORTING: "待报",      # 49
            xtconstant.ORDER_REPORTED: "已报",            # 50
            xtconstant.ORDER_REPORTED_CANCEL: "已报待撤", # 51
            xtconstant.ORDER_PARTSUCC_CANCEL: "部成待撤", # 52
            xtconstant.ORDER_PART_CANCEL: "部撤",         # 53
            xtconstant.ORDER_CANCELED: "已撤",            # 54
            xtconstant.ORDER_PART_SUCC: "部成",           # 55
            xtconstant.ORDER_SUCCEEDED: "已成",           # 56
            xtconstant.ORDER_JUNK: "废单",                # 57
            xtconstant.ORDER_UNKNOWN: "未知"              # 255
        }
        return status_dict.get(status_code, f"未知状态({status_code})")
    
    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        """
        self.order_count += 1
        self.logger.info(f"这是第{self.order_count}个委托回调")
        
        try:
            # 添加更多调试信息
            order_id = getattr(order, 'order_id', '未知ID')
            stock_code = getattr(order, 'stock_code', '未知代码')
            self.logger.debug(f"收到委托回调: order_id={order_id}, stock_code={stock_code}")
           
            # 解析委托信息
            order_action = "买入" if order.order_type == xtconstant.STOCK_BUY else "卖出"
            order_volume = order.order_volume
            order_price = order.price
            traded_volume = order.traded_volume
            order_status = self.get_order_status_text(order.order_status)
            remark = order.order_remark
            
            # 使用更详细的日志级别，确保所有信息都被记录
            message = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [委托回调] " \
                    f"{stock_code} {order_action} {order_volume}股@{order_price:.2f}, " \
                    f"已成交{traded_volume}股, 状态={order_status}, " \
                    f"订单编号={order_id}"
            
            self.logger.info(message)
            
            # 如果有备注，也记录下来
            if remark:
                self.logger.info(f"订单备注: {remark}")
                    
        except Exception as e:
            error_msg = f"委托回调异常: {e}"
            self.logger.error(error_msg)
            print(error_msg)  # 使用print确保输出到控制台
            try:
                self.logger.info(f"委托回调原始对象: {str(order)}")
            except:
                self.logger.info("无法打印委托回调对象")
    
    def on_stock_trade(self, trade):
        """
        成交变动推送
        :param trade: XtTrade对象
        """
        self.trade_count += 1
        self.logger.info(f"这是第{self.trade_count}个成交回调")
        
        try:
            # 解析成交信息
            stock_code = trade.stock_code
            # 修正：使用offset_flag判断买卖方向
            trade_action = "买入" if trade.order_type == xtconstant.STOCK_BUY else "卖出"
            trade_volume = trade.traded_volume
            trade_price = trade.traded_price
            trade_amount = trade.traded_amount
            trade_time = trade.traded_time
            order_id = trade.order_id
            trade_id = trade.traded_id  # 修正属性名
            
            # 显示成交信息（添加订单编号）
            self.logger.info(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [成交回调] "
                            f"{stock_code} {trade_action} {trade_volume}股@{trade_price:.2f}, "
                            f"成交时间={trade_time}, 订单ID={order_id}, 成交ID={trade_id}, "
                            f"成交金额={trade_amount:.2f}")
                
        except Exception as e:
            self.logger.error(f"成交回调异常: {e}")
            import traceback
            self.logger.error(f"异常堆栈: {traceback.format_exc()}")
    
    def on_order_error(self, order_error):
        """委托报错回调"""
        self.logger.error(f"委托报错: {order_error.error_msg}")


class QMTTrader:
    """QMT交易核心"""
    
    def __init__(self, config: Dict):
        self.config = config
        
        # 生成带日期的日志文件名
        today = datetime.datetime.now().strftime("%Y%m%d")
        log_dir = self.config.get('log_dir', 'logs')
        log_file = os.path.join(log_dir, f'QMTTrader_{today}.log')
        
        self.logger = setup_logger(
            name='QMTTrader', 
            log_level=self.config.get('log_level', 'INFO'), 
            log_file=log_file
        )
        
        # 初始化变量
        self.trader = None
        self.session_id = None
        self.account = None  # 添加账户对象
        self.connected = False
        self.callback_registered = False
        
        # 账户状态
        self.account_info = {}
        self.positions = {}
        
        # 连接QMT
        self._connect()

    def _connect(self):
        """连接QMT"""
        try:
            # 1. 设置交易路径和会话ID
            qmt_path = self.config['qmt_path']
            session_id = int(time.time() % 100000)  # 使用时间戳生成唯一会话ID
            
            # 2. 创建交易对象
            self.trader = XtQuantTrader(qmt_path, session_id)
            self.session_id = session_id
            
            # 3. 创建资金账号对象
            account_id = self.config['account_id']
            account_type = self.config['account_type']
            self.account = StockAccount(account_id, account_type)
            
            # 4. 创建回调对象并注册
            if not hasattr(self, 'callback'):       #确保回调对象只被创建和注册一次
                self.callback = QMTTraderCallback(self.logger)
                self.trader.register_callback(self.callback)
                self.logger.info("QMT回调对象已注册")
            else:
                self.logger.warning("QMT回调对象已存在,避免重复注册")
            
            # 5. 启动交易线程
            self.trader.start()
            
            # 6. 建立交易连接
            connect_result = self.trader.connect()
            if connect_result != 0:
                self.logger.error(f"连接失败，错误码: {connect_result}")
                return
            
            # 7. 订阅账号回调
            subscribe_result = self.trader.subscribe(self.account)
            if subscribe_result != 0:
                self.logger.error(f"订阅失败，错误码: {subscribe_result}")
                return
            
            self.connected = True
            self.logger.info("账户连接初始化成功！")
            
        except Exception as e:
            self.logger.error(f"连接QMT失败: {e}")

    def get_account_info(self) -> Dict:
        """获取账户信息"""
        if not self.connected:
            return {}
        
        try:
            # 使用self.account对象，而不是session_id和account_id
            asset = self.trader.query_stock_asset(self.account)
            if asset:
                self.account_info = {
                    'cash': asset.cash,
                    'total_asset': asset.total_asset,
                    'available_cash': getattr(asset, 'available_cash', asset.cash)
                }
                return self.account_info
        except Exception as e:
            self.logger.error(f"获取账户信息失败: {e}")
        
        return {}

    def get_positions(self) -> Dict:
        """获取持仓信息"""
        if not self.connected:
            return {}
        
        try:
            # 使用self.account对象，而不是session_id和account_id
            positions = self.trader.query_stock_positions(self.account)
            self.positions = {}
            for pos in positions:
                self.positions[pos.stock_code] = {
                    'volume': pos.volume,
                    'avg_price': pos.avg_price,
                    'market_value': pos.market_value
                }
            return self.positions
        except Exception as e:
            self.logger.error(f"获取持仓失败: {e}")
            return {}
    
    def get_market_data(self, symbols: List[str]) -> Dict:
        """获取市场数据"""
        try:
            data = {}
            for symbol in symbols:
                tick = xtdata.get_full_tick([symbol])
                if symbol in tick:
                    data[symbol] = {
                        'price': tick[symbol].get('last', 0),
                        'volume': tick[symbol].get('volume', 0),
                        'time': datetime.datetime.now()
                    }
            return data
        except Exception as e:
            self.logger.error(f"获取市场数据失败: {e}")
            return {}
    
    def execute_order_async(self, trade_signal: str, trade_quat: int, trade_price: float, trade_stockcode: str) -> bool:
        """异步下单执行方法"""
        # 确定订单类型
        if trade_signal == 'buy':
            order_type = xtconstant.STOCK_BUY
        else:
            order_type = xtconstant.STOCK_SELL
        
        try:
            # 使用官方异步下单API
            seq = self.trader.order_stock_async(
                account=self.account,  # 修改这里，使用self.account而不是配置中的字符串
                stock_code=trade_stockcode,
                order_type=order_type,
                order_volume=trade_quat,
                price_type=xtconstant.FIX_PRICE,
                price=trade_price,
                strategy_name="qmt_trader",
                order_remark=f"{trade_signal}_{trade_stockcode}"
            )
            
            # 检查下单请求是否成功发送
            if seq > 0:
                self.logger.info(f"异步下单请求已发送，序号: {seq}")
                return True
            else:
                self.logger.error(f"异步下单请求失败")
                return False
                
        except Exception as e:
            self.logger.error(f"下单异常: {e}")
            return False
    
    def disconnect(self):
        """断开连接"""
        if self.trader:
            try:
                self.trader.stop()
            except:
                pass
        self.connected = False
        self.logger.info("QMT连接已断开")
