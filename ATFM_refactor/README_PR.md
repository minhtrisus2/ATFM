from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, Iterator, Dict, Any
import pandas as pd

TIME_FMT_HM = "%H:%M"
DATE_FMT = "%Y-%m-%d"

def parse_schedule_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yêu cầu các cột: callsign, origin, destination, eobt, flight_date, aircraft_type
    eobt: HH:MM (UTC hoặc mốc bạn đang dùng nhất quán)
    flight_date: YYYY-MM-DD
    """
    required_cols = {"callsign", "origin", "destination", "eobt", "flight_date", "aircraft_type"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc trong schedule CSV: {missing}")
    # Chuẩn hoá chuỗi
    for col in ["callsign", "origin", "destination", "eobt", "flight_date", "aircraft_type"]:
        df[col] = df[col].astype(str).str.strip()
    return df

def _load_eets(path: Optional[str]) -> Optional[pd.DataFrame]:
    """
    eets.csv: airport_code,eet_to_vvts_minutes,eet_from_vvts_minutes,taxi_in_minutes,taxi_out_minutes
    """
    if not path:
        return None
    try:
        df = pd.read_csv(path)
        required = {"airport_code","eet_to_vvts_minutes","eet_from_vvts_minutes","taxi_in_minutes","taxi_out_minutes"}
        if not required.issubset(df.columns):
            return None
        # chuẩn hoá code
        df["airport_code"] = df["airport_code"].astype(str).str.strip().str.upper()
        return df
    except Exception:
        return None

def flight_dicts_from_df(
    df: pd.DataFrame,
    eets_csv_path: Optional[str] = "eets.csv",
) -> Iterator[Dict[str, Any]]:
    """
    Với flight ARRIVAL (destination == 'VVTS'):
      - Xác định ETA = (flight_date + eobt [đang là giờ rời chỗ giả định]) + EET_to_VVTS + taxi_in
      - Nếu không tìm thấy airport trong eets.csv → giữ nguyên mốc ban đầu, UI sẽ cảnh báo.
    Với flight DEPARTURE (origin == 'VVTS'):
      - Giữ eobt_utc như file schedule (mốc off-block).
    """
