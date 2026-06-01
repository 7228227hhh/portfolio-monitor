"""
Gamma组合模拟模块
根据实时行情计算虚拟组合的净Greeks
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from config import DEFAULT_COMBO_CONFIG


def find_atm_strike(df: pd.DataFrame) -> float:
    """找到最接近平值的行权价"""
    calls = df[df["option_type"] == "C"].copy()
    if calls.empty:
        return df["spot"].iloc[0]
    calls["delta_diff"] = abs(calls["delta"] - 0.5)
    atm_call = calls.loc[calls["delta_diff"].idxmin()]
    return atm_call["strike"]


def build_combo_positions(
    df: pd.DataFrame,
    combo_config: Dict = None
) -> pd.DataFrame:
    """
    根据组合配置，从期权链中选出具体的期权合约，并计算每腿的Greeks。
    """
    if combo_config is None:
        combo_config = DEFAULT_COMBO_CONFIG

    spot = df["spot"].iloc[0]
    atm_k = find_atm_strike(df)
    positions = []

    for leg in combo_config["legs"]:
        direction = -1 if leg["type"] == "SHORT" else 1
        ratio = leg["ratio"]
        option_spec = leg["option"]

        if option_spec == "ATM_STRADDLE":
            for opt_type in ["C", "P"]:
                row = df[
                    (df["strike"] == atm_k) &
                    (df["option_type"] == opt_type)
                ]
                if not row.empty:
                    r = row.iloc[0]
                    positions.append({
                        "leg": f"{leg['type']} ATM {opt_type}",
                        "option_type": opt_type,
                        "strike": atm_k,
                        "position": direction * ratio,
                        "delta": r["delta"] * direction * ratio,
                        "gamma": r["gamma"] * direction * ratio,
                        "vega": r["vega"] * direction * ratio,
                        "theta": r["theta"] * direction * ratio,
                    })

        elif option_spec.startswith("OTM_PUT_"):
            pct = float(option_spec.replace("OTM_PUT_", "")) / 100
            target_strike = spot * pct
            puts = df[df["option_type"] == "P"]
            if not puts.empty:
                idx = (puts["strike"] - target_strike).abs().argmin()
                r = puts.iloc[idx]
                positions.append({
                    "leg": f"{leg['type']} {option_spec}",
                    "option_type": "P",
                    "strike": r["strike"],
                    "position": direction * ratio,
                    "delta": r["delta"] * direction * ratio,
                    "gamma": r["gamma"] * direction * ratio,
                    "vega": r["vega"] * direction * ratio,
                    "theta": r["theta"] * direction * ratio,
                })

        elif option_spec.startswith("OTM_CALL_"):
            pct = float(option_spec.replace("OTM_CALL_", "")) / 100
            target_strike = spot * pct
            calls = df[df["option_type"] == "C"]
            if not calls.empty:
                idx = (calls["strike"] - target_strike).abs().argmin()
                r = calls.iloc[idx]
                positions.append({
                    "leg": f"{leg['type']} {option_spec}",
                    "option_type": "C",
                    "strike": r["strike"],
                    "position": direction * ratio,
                    "delta": r["delta"] * direction * ratio,
                    "gamma": r["gamma"] * direction * ratio,
                    "vega": r["vega"] * direction * ratio,
                    "theta": r["theta"] * direction * ratio,
                })

    return pd.DataFrame(positions)


def calculate_net_greeks(positions_df: pd.DataFrame) -> Dict[str, float]:
    """汇总组合的净Greeks"""
    if positions_df.empty:
        return {
            "net_delta": 0.0, "net_gamma": 0.0,
            "net_vega": 0.0, "net_theta": 0.0
        }
    return {
        "net_delta": positions_df["delta"].sum(),
        "net_gamma": positions_df["gamma"].sum(),
        "net_vega": positions_df["vega"].sum(),
        "net_theta": positions_df["theta"].sum(),
    }