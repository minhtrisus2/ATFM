
from __future__ import annotations
from datetime import datetime
from typing import Iterable, Dict
import pandas as pd

TIME_FMT_HM = "%H:%M"
DATE_FMT = "%Y-%m-%d"

def parse_schedule_df(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"callsign", "origin", "destination", "eobt", "flight_date", "aircraft_type"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc trong schedule CSV: {missing}")
    return df

def flight_dicts_from_df(df: pd.DataFrame):
    for _, row in df.iterrows():
        yield {
            "callsign": row["callsign"],
            "origin": row["origin"],
            "destination": row["destination"],
            "eobt_utc": datetime.strptime(f"{row['flight_date']} {row['eobt']}", f"{DATE_FMT} {TIME_FMT_HM}"),
            "aircraft": row["aircraft_type"],
            "is_arrival": (row["destination"] == "VVTS")
        }
