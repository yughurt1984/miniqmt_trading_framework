"""
主程序入口
"""
import time
import json
from datetime import datetime
from typing import Dict  # 添加这行导入
import os

# 导入模块
from qmt_trader import QMTTrader
from strategy import MovingAverageStrategy
import risk_control
from logger import setup_logger



def load_config(config_file: str = "config.json") -> Dict:
    """加载配置文件"""
    try:
        # 获取当前脚本的目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 构建配置文件的绝对路径
        config_path = os.path.join(current_dir, config_file)
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        print(f"尝试加载的配置文件路径: {config_path if 'config_path' in locals() else config_file}")
        return {}

# 在 main.py 中
def main():
    """主函数"""
     # 先加载配置
    config = load_config()
    if not config:
        print("配置加载失败，使用默认配置")
        config = {
            'log_name': 'miniqmt',
            'log_level': 'INFO',
            'log_file': 'test.log'
        }
    
    # 从配置中获取日志名称和日志级别
    log_name = config.get('log_name', 'miniqmt')
    log_level = config.get('log_level', 'INFO')
    log_file = config.get('log_file', 'test.log')

    # 初始化日志
    logger = setup_logger(name=log_name, log_level=log_level, log_file=log_file)

    logger.info("启动交易系统")
    
    
    # 初始化组件
    trader = QMTTrader(config)
    risk_manager = risk_control.RiskControl(config)
    
    # 初始化移动平均策略
    strategy = MovingAverageStrategy(config)  # 传递配置参数
    
    # 主循环
    running = True
    while running:
        try:
            # 获取账户信息
            account_info = trader.get_account_info()
            positions = trader.get_positions()
            
            # 获取市场数据
            market_data = trader.get_market_data(config.get('watch_list', []))
            
            # 准备策略数据
            strategy_data = {
                'market_data': market_data,
                'account_info': account_info,
                'positions': positions,
                'watch_list': config.get('watch_list', [])
            }
            
            # 生成交易信号
            signals = strategy.generate_signals(strategy_data)
            
            # 执行信号
            for signal in signals:
                # 风控检查
                passed, reason = risk_manager.check_signal(signal, account_info, positions)
                
                if passed:
                    # 执行交易
                    success = trader.execute_order_async(
                        trade_signal=signal['order_type'],
                        trade_quat=signal['quantity'],
                        trade_price=signal['price'],
                        trade_stockcode=signal['symbol']
                    )
                    
                    if success:
                        logger.info(f"信号执行成功: {signal}")
                else:
                    logger.warning(f"信号被风控拒绝: {reason}")
            
            # 系统状态日志（每分钟一次）
            if datetime.now().second == 0:
                 # 准备账户信息
                cash = account_info.get('cash', 0)
                total_asset = account_info.get('total_asset', cash)  # 如果没有总资产，使用现金
    
                # 准备持仓信息
                positions_info = []
                if positions:
                    for symbol, pos in positions.items():
                        volume = pos.get('volume', 0)
                        avg_price = pos.get('avg_price', 0)
                        market_value = pos.get('market_value', 0)
                        if volume > 0:  # 只显示有持仓的股票
                            positions_info.append(f"{symbol}:{volume}股(市值{market_value:.2f})")
    
                # 构建日志消息
                if positions_info:
                    positions_str = ", ".join(positions_info)
                    logger.info(f"系统状态: 账户现金={cash:.2f}, 总资产={total_asset:.2f}, 持仓[{positions_str}]")
                else:
                    logger.info(f"系统状态: 账户现金={cash:.2f}, 总资产={total_asset:.2f}, 无持仓")
    
            # 等待下一次循环
            time.sleep(config.get('interval', 5))
            
        except KeyboardInterrupt:
            logger.info("收到中断信号，准备退出")
            running = False
        except Exception as e:
            logger.error(f"主循环异常: {e}")
            time.sleep(10)  # 异常后等待10秒
    
    # 关闭系统
    trader.disconnect()
    logger.info("交易系统已关闭")


if __name__ == "__main__":
    main()