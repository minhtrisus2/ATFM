import os
import pandas as pd
import streamlit as st
from datetime import timedelta
from .config import VVTS_CONFIG

@st.cache_data
def load_and_prepare_data():
    try:
        current_script_path = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_script_path))
        data_dir = os.path.join(project_root, 'data')
        
        schedule_path = os.path.join(data_dir, 'vvts_schedule.csv')
        eets_path = os.path.join(data_dir, 'eets.csv')

        if not os.path.exists(schedule_path) or not os.path.exists(eets_path):
            st.error(f"Lỗi: Không tìm thấy file dữ liệu trong '{data_dir}'. Vui lòng chạy 'generate_data.py' trước.")
            return None
            
        flights_df = pd.read_csv(schedule_path)
        eets_df = pd.read_csv(eets_path)
        
        flights_df['eobt_local'] = pd.to_datetime(flights_df['flight_date'] + ' ' + flights_df['eobt'], format='%Y-%m-%d %H:%M', errors='coerce')
        flights_df.dropna(subset=['eobt_local'], inplace=True)
        flights_df['eobt_utc'] = flights_df['eobt_local'] - timedelta(hours=VVTS_CONFIG['TIMEZONE_OFFSET_HOURS'])
        flights_df['flight_date'] = flights_df['eobt_local'].dt.date

        flights_df = pd.merge(flights_df, eets_df, left_on='origin', right_on='airport_code', how='left', suffixes=('', '_origin'))
        flights_df = pd.merge(flights_df, eets_df, left_on='destination', right_on='airport_code', how='left', suffixes=('', '_dest'))

        return flights_df.drop(columns=['airport_code', 'airport_code_dest'], errors='ignore')

    except Exception as e:
        st.error(f"Lỗi nghiêm trọng khi tải dữ liệu: {e}")
        return None