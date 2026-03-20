# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 13:35:26 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import logging
import pandas as pd
import numpy as np

class PositionManager:
    """
    A股短线量化持仓管理器 (核心层)
    严控 T+1 / T+2 周期状态流转与可用余额核算
    """
    REQUIRED_COLUMNS = [
        'code', 'total_qty', 'available_qty', 'frozen_qty', 
        'cost_price', 'latest_price', 'hold_days', 'position_status',
        'unrealized_pnl', 'unrealized_pnl_ratio'
    ]

    def __init__(self, target_date: str, close_positions_file: str):
        self.target_date = target_date
        self.close_positions_file = close_positions_file
        self.logger = logging.getLogger("PositionManager")
        self.positions = self._load_positions()

    def _load_positions(self) -> pd.DataFrame:
        if not os.path.exists(self.close_positions_file):
            self.logger.warning(f"Close positions file not found at {self.close_positions_file}. Initializing empty.")
            return pd.DataFrame(columns=self.REQUIRED_COLUMNS)
        
        df = pd.read_csv(self.close_positions_file, dtype={'code': str})
        
        # 字段映射兼容 (Stage 08 产出可能带有不同前缀)
        if 'symbol' in df.columns and 'code' not in df.columns:
            df = df.rename(columns={'symbol': 'code'})
        if 'filled_shares' in df.columns and 'total_qty' not in df.columns:
            df['total_qty'] = df['filled_shares']
            df['available_qty'] = 0  # 默认今日买入，暂不可用
            df['frozen_qty'] = df['filled_shares']
        if 'avg_fill_price' in df.columns and 'cost_price' not in df.columns:
            df['cost_price'] = df['avg_fill_price']
        if 'close_price' in df.columns and 'latest_price' not in df.columns:
            df['latest_price'] = df['close_price']
        if 'hold_days' not in df.columns:
            df['hold_days'] = 0
            
        missing = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            # 容错：补充缺失列
            for col in missing:
                df[col] = 0.0 if 'qty' in col or 'price' in col or 'pnl' in col else 'UNKNOWN'
                
        return df

    def rollover_t1_positions(self) -> pd.DataFrame:
        """
        执行次日开盘前的 T+1 状态流转:
        1. 冻结股数 (昨日新买入) 转移至 可用股数
        2. 持仓天数 +1
        """
        if self.positions.empty:
            return self.positions.copy()

        df = self.positions.copy()
        
        # 强制类型转换保证计算安全
        df['available_qty'] = pd.to_numeric(df['available_qty'], errors='coerce').fillna(0)
        df['frozen_qty'] = pd.to_numeric(df['frozen_qty'], errors='coerce').fillna(0)
        df['hold_days'] = pd.to_numeric(df['hold_days'], errors='coerce').fillna(0)

        # T+1 流转：昨日冻结转为今日可用
        df['available_qty'] = df['available_qty'] + df['frozen_qty']
        df['frozen_qty'] = 0 
        
        # 增加持仓天数 (过夜即+1)
        df['hold_days'] = df['hold_days'] + 1
        
        # 状态重置
        df.loc[df['total_qty'] <= 0, 'position_status'] = 'CLEARED'
        df.loc[df['total_qty'] > 0, 'position_status'] = 'HOLDING'
        
        return df

    def generate_next_day_plan(self, df_rolled: pd.DataFrame, risk_rules: dict) -> pd.DataFrame:
        """
        基于风控规则与最新持仓状态，生成次日续管决策
        """
        if df_rolled.empty:
            return pd.DataFrame()

        df_plan = df_rolled[df_rolled['position_status'] == 'HOLDING'].copy()
        if df_plan.empty:
            return df_plan
            
        # 默认次日续管状态
        df_plan['next_day_action'] = 'HOLD'
        df_plan['action_reason'] = 'NORMAL'

        # 胜率优先逻辑：严格止盈止损判定
        take_profit_ratio = risk_rules.get('take_profit_ratio', 0.05)
        stop_loss_ratio = risk_rules.get('stop_loss_ratio', -0.03)
        max_hold_days = risk_rules.get('max_hold_days', 2)

        df_plan['unrealized_pnl_ratio'] = pd.to_numeric(df_plan['unrealized_pnl_ratio'], errors='coerce').fillna(0.0)

        # 止盈触发
        tp_mask = df_plan['unrealized_pnl_ratio'] >= take_profit_ratio
        df_plan.loc[tp_mask, 'next_day_action'] = 'SELL'
        df_plan.loc[tp_mask, 'action_reason'] = 'TAKE_PROFIT'

        # 止损触发
        sl_mask = df_plan['unrealized_pnl_ratio'] <= stop_loss_ratio
        df_plan.loc[sl_mask, 'next_day_action'] = 'SELL'
        df_plan.loc[sl_mask, 'action_reason'] = 'STOP_LOSS'

        # 时间止损触发 (T+N 强制离场)
        time_mask = (df_plan['hold_days'] >= max_hold_days) & (df_plan['next_day_action'] == 'HOLD')
        df_plan.loc[time_mask, 'next_day_action'] = 'SELL'
        df_plan.loc[time_mask, 'action_reason'] = 'TIME_EXPIRED'

        # T+1 锁仓拦截: 确保只有可用仓位才能被执行卖出 (实盘底线防呆)
        locked_mask = (df_plan['next_day_action'] == 'SELL') & (df_plan['available_qty'] <= 0)
        df_plan.loc[locked_mask, 'next_day_action'] = 'HOLD_LOCKED'
        df_plan.loc[locked_mask, 'action_reason'] = 'T1_LOCKED'

        return df_plan