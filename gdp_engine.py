# atfm_core/gdp_engine.py

import pandas as pd
from datetime import timedelta
from .config import VVTS_CONFIG

def run_gdp_simulation(master_schedule_df, landing_capacity, arr_hotspots):
    """
    Chạy mô phỏng GDP cho các chuyến bay bị ảnh hưởng bởi các điểm nóng.
    """
    if master_schedule_df.empty or arr_hotspots.empty:
        return master_schedule_df

    df = master_schedule_df.copy()
    
    df['regulated_time_utc'] = df['event_time_utc']
    df['is_regulated'] = False
    
    last_available_slot = pd.Timestamp.min.tz_localize('UTC')

    for hour, row in arr_hotspots.iterrows():
        demand = int(row['arrival_demand'])
        capacity = int(row['landing_capacity'])
        overload = demand - capacity

        if overload <= 0:
            continue

        start_hour_utc = hour.tz_convert('UTC')
        end_hour_utc = start_hour_utc + timedelta(hours=1)
        
        flights_in_hour = df[
            (df['flight_type'] == 'arrival') &
            (df['event_time_utc'] >= start_hour_utc) &
            (df['event_time_utc'] < end_hour_utc)
        ].copy()

        flights_in_hour.sort_values(by='event_time_utc', ascending=False, inplace=True)
        flights_to_delay = flights_in_hour.head(overload)
        
        if last_available_slot < end_hour_utc:
            last_available_slot = end_hour_utc
            
        slot_interval = timedelta(minutes=60 / capacity)

        for index, flight in flights_to_delay.sort_values(by='event_time_utc').iterrows():
            new_slot = last_available_slot + slot_interval
            df.loc[index, 'regulated_time_utc'] = new_slot
            df.loc[index, 'is_regulated'] = True
            last_available_slot = new_slot

    df['atfm_delay_minutes'] = (df['regulated_time_utc'] - df['event_time_utc']).dt.total_seconds() / 60
    df.loc[df['atfm_delay_minutes'] < 0, 'atfm_delay_minutes'] = 0
    return df

def format_gdp_results(regulated_df):
    """
    Định dạng DataFrame kết quả cuối cùng để hiển thị, tính toán CTOT.
    SỬA LỖI: Loại bỏ lệnh .tz_localize không cần thiết cho cột ctot_utc.
    """
    if regulated_df is None or regulated_df.empty:
        return pd.DataFrame()

    df = regulated_df.copy()
    tz_offset = timedelta(hours=VVTS_CONFIG['TIMEZONE_OFFSET_HOURS'])
    
    # Các cột _utc đã là timezone-aware, chỉ cần convert
    df['regulated_time_local'] = df['regulated_time_utc'].dt.tz_convert(f'Etc/GMT-{tz_offset.seconds // 3600}').dt.tz_localize(None)
    df['original_event_time_local'] = df['event_time_utc'].dt.tz_convert(f'Etc/GMT-{tz_offset.seconds // 3600}').dt.tz_localize(None)
    
    # Tính toán CTOT cho các chuyến bay đến bị điều tiết
    arr_mask = (df['flight_type'] == 'arrival') & (df['is_regulated'])
    if arr_mask.any():
        eet_delta = pd.to_timedelta(df.loc[arr_mask, 'eet_minutes'], unit='m')
        df.loc[arr_mask, 'ctot_utc'] = df.loc[arr_mask, 'regulated_time_utc'] - eet_delta
    
    # Format giờ để hiển thị
    if 'ctot_utc' in df.columns:
        # SỬA LỖI: Cột ctot_utc đã là timezone-aware, chỉ cần convert
        df['ctot_new_local'] = pd.to_datetime(df['ctot_utc']).dt.tz_convert(f'Etc/GMT-{tz_offset.seconds // 3600}').dt.strftime('%H:%M').fillna('--:--')
    else:
        df['ctot_new_local'] = '--:--'
        
    df['new_scheduled_time_local'] = df['regulated_time_local'].dt.strftime('%H:%M').fillna('--:--')
    df['original_scheduled_time_local'] = df['original_event_time_local'].dt.strftime('%H:%M').fillna('--:--')
    
    df['atfm_delay_minutes'] = df['atfm_delay_minutes'].round(0).astype(int)
    return df