# atfm_core/flight_processing.py

import pandas as pd
from datetime import timedelta
from .config import VVTS_CONFIG, get_master_dataframe_schema

def process_flight_schedules(raw_flights_df):
    """
    SỬA LỖI: Chuẩn hóa tất cả các cột UTC thành timezone-aware ngay từ đầu.
    """
    if raw_flights_df is None or raw_flights_df.empty:
        return get_master_dataframe_schema()

    df = raw_flights_df.copy()
    
    # Chuẩn hóa múi giờ cho cột đầu vào
    df['eobt_utc'] = pd.to_datetime(df['eobt_utc']).dt.tz_localize('UTC')
    
    processed_flights = []
    for _, row in df.iterrows():
        flight_data = {
            'callsign': row['callsign'], 'origin': row['origin'], 'destination': row['destination'],
            'aircraft_type': row['aircraft_type'], 'flight_date': row['flight_date'],
            'eobt_utc': row['eobt_utc'], 'eobt_local': row['eobt_local'],
            'eet_minutes': 0, # Khởi tạo
        }

        if row['destination'] == VVTS_CONFIG['ICAO_CODE']:
            flight_data['flight_type'] = 'arrival'
            flight_data['eet_minutes'] = int(row.get('origin_eet_to_vvts_minutes', 90))
            flight_data['etot_utc'] = row['eobt_utc'] + timedelta(minutes=row.get('origin_taxi_out_minutes', 15))
            flight_data['eldt_utc'] = flight_data['etot_utc'] + timedelta(minutes=flight_data['eet_minutes'])
        elif row['origin'] == VVTS_CONFIG['ICAO_CODE']:
            flight_data['flight_type'] = 'departure'
            flight_data['eet_minutes'] = int(row.get('dest_eet_from_vvts_minutes', 90))
            flight_data['etot_utc'] = row['eobt_utc'] + timedelta(minutes=VVTS_CONFIG['TAXI_OUT_TIME_MINUTES'])
            flight_data['eldt_utc'] = flight_data['etot_utc'] + timedelta(minutes=flight_data['eet_minutes'])
        else:
            continue
        
        processed_flights.append(flight_data)

    if not processed_flights:
        return get_master_dataframe_schema()

    master_df = pd.DataFrame(processed_flights)
    
    master_df['event_time_utc'] = master_df.apply(lambda r: r['eldt_utc'] if r['flight_type'] == 'arrival' else r['etot_utc'], axis=1)
    
    # Tạo cột event_time_local để hiển thị
    tz_offset = timedelta(hours=VVTS_CONFIG['TIMEZONE_OFFSET_HOURS'])
    master_df['event_time_local'] = master_df['event_time_utc'].dt.tz_convert(f'Etc/GMT-{tz_offset.seconds // 3600}').dt.tz_localize(None)
    
    master_df.sort_values(by='event_time_utc', inplace=True)
    return master_df