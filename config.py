import pandas as pd

VVTS_CONFIG = {
    "ICAO_CODE": "VVTS",
    "TAKEOFF_CAPACITY_HOURLY": 24,
    "LANDING_CAPACITY_HOURLY": 24,
    "TAXI_OUT_TIME_MINUTES": 15,
    "TIMEZONE_OFFSET_HOURS": 7
}

def get_master_dataframe_schema():
    columns_with_types = {
        'callsign': str, 'origin': str, 'destination': str, 'aircraft_type': str,
        'flight_date': 'datetime64[ns]', 'eobt_utc': 'datetime64[ns]', 'eobt_local': 'datetime64[ns]',
        'etot_utc': 'datetime64[ns]', 'eet_minutes': int, 'eldt_utc': 'datetime64[ns]',
        'flight_type': str, 'event_time_utc': 'datetime64[ns]'
    }
    return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns_with_types.items()})