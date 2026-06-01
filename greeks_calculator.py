"""
衍生指标计算模块
计算GEX、DIX、偏度、期限结构等聚合指标
"""
import pandas as pd
import numpy as np
from typing import Dict, Any


def calculate_atm_iv(df: pd.DataFrame) -> float:
    """计算ATM隐含波动率（delta最接近0.5的Call的IV）"""
    calls = df[df["option_type"] == "C"].copy()
    if calls.empty:
        return np.nan
    calls["delta_diff"] = abs(calls["delta"] - 0.5)
    atm_row = calls.loc[calls["delta_diff"].idxmin()] #找到 calls 这个 DataFrame 中 "delta_diff" 列的最小值所在的那一行，然后把这一整行数据赋值给变量 atm_row。
    return float(atm_row.get("implied_vol",0.0))


def calculate_skew(df: pd.DataFrame) -> float:
    """计算25-Delta偏度 = IV(25Δ Put) - IV(25Δ Call)"""
    """TODO：这里的算法有问题，问题在于没有用插值精确插到25delta的iv，只是一个近似，后续需要补充"""
    puts = df[df["option_type"] == "P"].copy()
    calls = df[df["option_type"] == "C"].copy()
    if puts.empty or calls.empty:
        return np.nan

    puts["delta_diff"] = abs(puts["delta"] - (-0.25))
    calls["delta_diff"] = abs(calls["delta"] - 0.25)

    put_iv = puts.loc[puts["delta_diff"].idxmin(), "implied_vol"]
    call_iv = calls.loc[calls["delta_diff"].idxmin(), "implied_vol"]

    return put_iv - call_iv


def calculate_term_structure_slope(df: pd.DataFrame) -> float:
    """期限结构斜率：近期ATM IV - 远期ATM IV"""
    if df.empty:
        return np.nan

    expiry_groups = df.groupby("expiry_date")
    atm_ivs = []

    for expiry, group in expiry_groups:
        iv = calculate_atm_iv(group)
        if not np.isnan(iv):
            atm_ivs.append({
                "expiry_date": pd.Timestamp(expiry),
                "days_left": group["days_left"].iloc[0],
                "atm_iv": iv
            })

    if len(atm_ivs) < 2:
        return np.nan

    iv_df = pd.DataFrame(atm_ivs).sort_values("days_left")
    slope = iv_df.iloc[0]["atm_iv"] - iv_df.iloc[-1]["atm_iv"]
    return slope


def calculate_gex(df: pd.DataFrame,multiplier) -> float:
    """TODO：需要考虑Gamma的方向问题"""
    total_gex = 0.0
    for _, row in df.iterrows():
        spot = row["spot"]
        gamma = row["gamma"]
        total_gex -= gamma * spot * multiplier
    return total_gex


def calculate_dix(df: pd.DataFrame,multiplier) -> float:
    """TODO:需要考虑delta的方向问题"""
    total_dix = 0.0
    for _, row in df.iterrows():
        delta = row["delta"]
        total_dix += delta * multiplier
    return total_dix


def calculate_all_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """计算单个标的所有聚合指标"""
    return {
        "spot": df["spot"].iloc[0] if not df.empty else np.nan,
        "atm_iv": calculate_atm_iv(df),
        "skew_25d": calculate_skew(df),
        "term_slope": calculate_term_structure_slope(df),
        "gex": calculate_gex(df,multiplier=100),
        "dix": calculate_dix(df,multiplier=100),
        "num_options": len(df),
        "num_expiries": df["expiry_date"].nunique(),
    }