import pandas as pd

class ChanMerge:
    def __init__(self):
        self.merged_bars = []  # 存储最终合并后的标准化K线
        self.direction = 0     # 1: 向上, -1: 向下, 0: 初始未定
        self.base_bar = None   # 当前作为基准的标准化K线

    def handle_bar(self, current_bar):
        """
        处理单根新K线 (实盘逻辑：逐根进入)
        current_bar: dict 包含 'date', 'high', 'low'
        """
        if self.base_bar is None:
            self.base_bar = current_bar.copy()
            return

        # 1. 确定初始方向 (仅在最开始两根非包含K线之间确定)
        if self.direction == 0:
            if current_bar['high'] > self.base_bar['high'] and current_bar['low'] > self.base_bar['low']:
                self.direction = 1
            elif current_bar['low'] < self.base_bar['low'] and current_bar['high'] < self.base_bar['high']:
                self.direction = -1
            
            # 如果初始阶段就存在包含，按非包含处理或等待
            if self.direction == 0:
                # 初始包含处理：暂时以第一根为准，合并逻辑取决于谁包含谁
                is_cont = self._check_containing(self.base_bar, current_bar)
                if not is_cont:
                    self.merged_bars.append(self.base_bar)
                    self.base_bar = current_bar.copy()
                return

        # 2. 包含关系判定
        is_containing = self._check_containing(self.base_bar, current_bar)

        if is_containing:
            # 包含：根据当前方向执行合并
            if self.direction == 1: # 向上趋势：高高原则 (低点取高点)
                self.base_bar['high'] = max(self.base_bar['high'], current_bar['high'])
                self.base_bar['low'] = max(self.base_bar['low'], current_bar['low'])
            else: # 向下趋势：低低原则
                self.base_bar['high'] = min(self.base_bar['high'], current_bar['high'])
                self.base_bar['low'] = min(self.base_bar['low'], current_bar['low'])
            # 合并后日期更新为最新日期
            self.base_bar['date'] = current_bar['date']
        else:
            # 非包含：基准K线完成，存入结果
            self.merged_bars.append(self.base_bar.copy())
            
            # 更新方向：根据新K线与旧基准K线关系
            if current_bar['high'] > self.base_bar['high']:
                self.direction = 1
            elif current_bar['low'] < self.base_bar['low']:
                self.direction = -1
            
            # 切换基准
            self.base_bar = current_bar.copy()

    def _check_containing(self, bar1, bar2):
        """判定包含关系"""
        # bar1 包含 bar2 或 bar2 包含 bar1
        cond1 = (bar1['high'] >= bar2['high'] and bar1['low'] <= bar2['low'])
        cond2 = (bar2['high'] >= bar1['high'] and bar2['low'] <= bar1['low'])
        return cond1 or cond2

    def get_result(self):
        # 别忘了最后一根还没放进去的
        if self.base_bar:
            res = self.merged_bars + [self.base_bar]
            return pd.DataFrame(res)
        return pd.DataFrame()

def process_399300_data():
    """使用新的ChanMerge逻辑处理399300数据"""
    
    # 加载原始数据
    df = pd.read_csv("/home/mystic/workspace/quant/data/399300_2015_adj.csv")
    df['date'] = pd.to_datetime(df['date'])
    
    # 准备数据格式
    data = []
    for _, row in df.iterrows():
        data.append({
            "date": row['date'],
            "high": row['high'],
            "low": row['low'],
            "open": row['open'],
            "close": row['close'],
            "volume": row['volume'],
            "turnover": row['turnover'],
            "open_adj": row['open_adj'],
            "high_adj": row['high_adj'],
            "low_adj": row['low_adj'],
            "close_adj": row['close_adj'],
            "volume_adj": row['volume_adj'],
            "turnover_adj": row['turnover_adj'],
            "daily_return": row['daily_return'],
            "daily_return_adj": row['daily_return_adj'],
            "cum_return": row['cum_return'],
            "cum_return_adj": row['cum_return_adj']
        })
    
    print(f"=== 使用新ChanMerge逻辑处理399300数据 ===")
    print(f"原始数据: {len(data)} 根K线")
    
    # 使用新的ChanMerge算法
    engine = ChanMerge()
    
    # 需要修改handle_bar方法以处理完整的数据
    for bar in data:
        engine.handle_bar_full(bar)
    
    df_result = engine.get_result_full()
    
    print(f"合并后数据: {len(df_result)} 根K线")
    print(f"压缩率: {(1-len(df_result)/len(data))*100:.1f}%")
    
    # 保存结果，只保存指定字段
    output_file = "399300_2015_chan_merged_new.csv"
    df_result[['date', 'high', 'low', 'open', 'close', 'volume']].to_csv(output_file, index=False)
    print(f"结果已保存到: {output_file}")
    
    # 显示前10条结果
    print(f"\n前10条合并结果:")
    print(df_result[['date', 'high', 'low', 'open', 'close', 'volume']].head(10).to_string(index=False))
    
    return df_result

# 扩展ChanMerge类以处理完整数据
class ChanMerge(ChanMerge):
    def handle_bar_full(self, current_bar):
        """处理包含完整数据的K线"""
        if self.base_bar is None:
            self.base_bar = current_bar.copy()
            return

        # 1. 确定初始方向
        if self.direction == 0:
            if current_bar['high'] > self.base_bar['high'] and current_bar['low'] > self.base_bar['low']:
                self.direction = 1
            elif current_bar['low'] < self.base_bar['low'] and current_bar['high'] < self.base_bar['high']:
                self.direction = -1
            
            if self.direction == 0:
                is_cont = self._check_containing(self.base_bar, current_bar)
                if not is_cont:
                    self.merged_bars.append(self.base_bar)
                    self.base_bar = current_bar.copy()
                return

        # 2. 包含关系判定
        is_containing = self._check_containing(self.base_bar, current_bar)

        if is_containing:
            # 包含：根据当前方向执行合并
            if self.direction == 1: # 向上趋势：高高原则
                self.base_bar['high'] = max(self.base_bar['high'], current_bar['high'])
                self.base_bar['low'] = max(self.base_bar['low'], current_bar['low'])
            else: # 向下趋势：低低原则
                self.base_bar['high'] = min(self.base_bar['high'], current_bar['high'])
                self.base_bar['low'] = min(self.base_bar['low'], current_bar['low'])
            
            # 更新其他字段为最新值，volume进行累加
            self.base_bar['date'] = current_bar['date']
            self.base_bar['close'] = current_bar['close']
            self.base_bar['volume'] = self.base_bar['volume'] + current_bar['volume']  # volume累加
            self.base_bar['turnover'] = current_bar['turnover']
            self.base_bar['open_adj'] = current_bar['open_adj']
            self.base_bar['high_adj'] = current_bar['high_adj']
            self.base_bar['low_adj'] = current_bar['low_adj']
            self.base_bar['close_adj'] = current_bar['close_adj']
            self.base_bar['volume_adj'] = current_bar['volume_adj']
            self.base_bar['turnover_adj'] = current_bar['turnover_adj']
            self.base_bar['daily_return'] = current_bar['daily_return']
            self.base_bar['daily_return_adj'] = current_bar['daily_return_adj']
            self.base_bar['cum_return'] = current_bar['cum_return']
            self.base_bar['cum_return_adj'] = current_bar['cum_return_adj']
        else:
            # 非包含：基准K线完成，存入结果
            self.merged_bars.append(self.base_bar.copy())
            
            # 更新方向
            if current_bar['high'] > self.base_bar['high']:
                self.direction = 1
            elif current_bar['low'] < self.base_bar['low']:
                self.direction = -1
            
            # 切换基准
            self.base_bar = current_bar.copy()

    def get_result_full(self):
        """获取包含完整数据的合并结果"""
        if self.base_bar:
            res = self.merged_bars + [self.base_bar]
            return pd.DataFrame(res)
        return pd.DataFrame()

if __name__ == "__main__":
    result = process_399300_data()

