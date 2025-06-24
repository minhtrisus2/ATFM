import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, time, date
import os
import random

# --- Cấu hình trang và Hằng số Toàn cục ---
st.set_page_config(page_title="ATFM Simulation Dashboard - VVTS (Hoàn Chỉnh)", layout="wide")

VVTS_CONFIG = {
    "takeoff_capacity_hourly": 24,  # Năng lực cất cánh mặc định mỗi giờ
    "landing_capacity_hourly": 24,  # Năng lực hạ cánh mặc định mỗi giờ
    "taxi_out_time_minutes": 15,    # Thời gian lăn bánh ra từ cổng đến đường băng (cho DEP từ VVTS)
    "min_separation_minutes": 2,    # Khoảng cách thời gian tối thiểu giữa 2 lượt cất/hạ cánh
    "airport_timezone_offset_hours": 7 # Múi giờ của sân bay (UTC+7 cho VN)
}

# Hàm giúp khởi tạo DataFrame rỗng với đúng schema và dtypes
def get_empty_display_dataframe_schema():
    columns_with_types = {
        'callsign': str, 'origin': str, 'destination': str, 'aircraft_type': str,
        'eobt_dt_local': 'datetime64[ns]', 'eobt_dt_utc': 'datetime64[ns]',
        'eldt_dt_utc': 'datetime64[ns]', 'eet_minutes': float, 'eet_delta': 'timedelta64[ns]',
        'cldt_dt_utc': 'datetime64[ns]', 'etot_dt_utc': 'datetime64[ns]',
        'atfm_delay_minutes': float, 'is_regulated': bool, 'flight_type': str,
        'regulated_time_utc': 'datetime64[ns]', 'original_event_time_utc': 'datetime64[ns]',
        'regulated_time_local': 'datetime64[ns]', 'original_event_time_local': 'datetime64[ns]',
        'eobt_dt_local_display': str, 'ctot_new_local': str,
        'new_scheduled_time_local': 'datetime64[ns]', 'original_scheduled_time_local': str,

        # Các cột từ eets_df_merged để đảm bảo schema đủ cho strategic data
        'eet_to_vvts_minutes': float, 'eet_from_vvts_minutes': float,
        'taxi_in_minutes': float, 'taxi_out_minutes': float,
        'origin_eet_to_vvts_minutes': float, 'origin_eet_from_vvts_minutes': float, 'origin_taxi_in_minutes': float, 'origin_taxi_out_minutes': float,
        'dest_eet_to_vvts_minutes': float, 'dest_eet_from_vvts_minutes': float, 'dest_taxi_in_minutes': float, 'dest_taxi_out_minutes': float,

        # Các cột mới tính toán trong calculate_initial_schedules
        'etot_origin_dt_utc': 'datetime64[ns]', 'eet_to_vvts_delta': 'timedelta64[ns]', 'eibt_vvts_dt_utc': 'datetime64[ns]',
        'eet_from_vvts_delta': 'timedelta64[ns]', 'eldt_at_dest_dt_utc': 'datetime64[ns]', 'eibt_at_dest_dt_utc': 'datetime64[ns]',

        # Các cột cho Strategic Data Display (dạng string)
        'EOBT_local_str': str, 'ETOT_local_str': str, 'ELDT_local_str': str, 'EIBT_local_str': str,
        'EOBT_origin_local_str': str, 'ETOT_origin_local_str': str, 'ELDT_vvts_dt_local_str': str, 'EIBT_vvts_dt_local_str': str,

        # Các cột cho Pre-Tactical Demand Data
        'predicted_event_time_utc': 'datetime64[ns]',
        'predicted_event_time_local': 'datetime64[ns]',
        'is_predicted_delayed': bool,
        'prediction_delay_minutes': float
    }

    empty_df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns_with_types.items()})
    return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns_with_types.items()})

# --- Các Hàm Xử Lý Dữ Liệu (Core Logic) ---

@st.cache_data
def load_data():
    """
    Tải dữ liệu từ các file CSV và tiền xử lý.
    Cột 'aircraft_type' trong vvts_schedule.csv.
    Các cột 'airport_code', 'eet_to_vvts_minutes', 'eet_from_vvts_minutes', 'taxi_in_minutes', 'taxi_out_minutes' trong eets.csv.
    """
    try:
        script_dir = os.path.dirname(os.path.realpath(__file__))
        schedule_path = os.path.join(script_dir, 'vvts_schedule.csv')
        eets_path = os.path.join(script_dir, 'eets.csv')

        flights_df = pd.read_csv(schedule_path)
        eets_df = pd.read_csv(eets_path)

        flights_df['eobt_dt_local'] = pd.to_datetime(flights_df['flight_date'] + ' ' + flights_df['eobt'], format='%Y-%m-%d %H:%M', errors='coerce')
        flights_df.dropna(subset=['eobt_dt_local'], inplace=True)
        flights_df['eobt_dt_utc'] = flights_df['eobt_dt_local'] - timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])

        if 'aircraft_type' not in flights_df.columns:
            flights_df['aircraft_type'] = 'N/A'
            st.warning("File 'vvts_schedule.csv' thiếu cột 'aircraft_type'. Đã thêm cột rỗng.")

        # --- FIX: Hợp nhất dữ liệu từ eets_df một cách an toàn và rõ ràng ---

        # Merge thông tin của sân bay origin vào flights_df
        eets_origin_rename_map = {
            'airport_code': 'origin_airport_code',
            'eet_to_vvts_minutes': 'origin_eet_to_vvts_minutes',
            'eet_from_vvts_minutes': 'origin_eet_from_vvts_minutes',
            'taxi_in_minutes': 'origin_taxi_in_minutes',
            'taxi_out_minutes': 'origin_taxi_out_minutes'
        }
        flights_df = pd.merge(flights_df, eets_df.rename(columns=eets_origin_rename_map),
                                  left_on='origin', right_on='origin_airport_code', how='left')
        flights_df.drop(columns=['origin_airport_code'], inplace=True, errors='ignore')

        # Merge thông tin của sân bay destination vào flights_df
        eets_dest_rename_map = {
            'airport_code': 'dest_airport_code',
            'eet_to_vvts_minutes': 'dest_eet_to_vvts_minutes',
            'eet_from_vvts_minutes': 'dest_eet_from_vvts_minutes',
            'taxi_in_minutes': 'dest_taxi_in_minutes',
            'taxi_out_minutes': 'dest_taxi_out_minutes'
        }
        flights_df = pd.merge(flights_df, eets_df.rename(columns=eets_dest_rename_map),
                                  left_on='destination', right_on='dest_airport_code', how='left')
        flights_df.drop(columns=['dest_airport_code'], inplace=True, errors='ignore')

        # Điền các giá trị NaN cuối cùng sau khi merge xong
        for col in ['origin_eet_to_vvts_minutes', 'origin_eet_from_vvts_minutes', 'origin_taxi_in_minutes', 'origin_taxi_out_minutes',
                    'dest_eet_to_vvts_minutes', 'dest_eet_from_vvts_minutes', 'dest_taxi_in_minutes', 'dest_taxi_out_minutes']:
            flights_df[col] = pd.to_numeric(flights_df[col], errors='coerce').fillna(15 if 'taxi' in col else 60)

        return flights_df, eets_df
    except FileNotFoundError:
        st.error("Không tìm thấy file dữ liệu. Vui lòng đảm bảo 'vvts_schedule.csv' (có cột 'flight_date' và 'aircraft_type') và 'eets.csv' nằm cùng thư mục với ứng dụng.")
        return None, None
    except KeyError as e:
        st.error(f"Lỗi: Thiếu cột dữ liệu cần thiết trong CSV hoặc lỗi trong quá trình hợp nhất dữ liệu. Vui lòng kiểm tra cấu trúc file 'vvts_schedule.csv' và 'eets.csv'. Lỗi: {e}")
        return None, None
    except Exception as e:
        st.error(f"Lỗi khi tải hoặc xử lý dữ liệu: {e}. Vui lòng kiểm tra định dạng file và dữ liệu.")
        return None, None

def calculate_initial_schedules(flights_df, eets_df):
    """
    Tính toán lịch trình ban đầu (ELDT cho chuyến đến, ETOT cho chuyến đi).
    Tất cả các thời gian được tính toán và lưu trữ ở múi giờ UTC để nhất quán.
    Bổ sung tính toán ELDT/EIBT cho DEP và EIBT cho ARR cho mục Strategic Data.
    """
    # Xử lý chuyến đến (Arrivals)
    arrivals_df = flights_df[flights_df['destination'] == 'VVTS'].copy()

    # ETOT at Origin for Arrivals = EOBT at Origin + Taxi-out at Origin
    arrivals_df['origin_taxi_out_delta'] = pd.to_timedelta(arrivals_df['origin_taxi_out_minutes'], unit='m', errors='coerce')
    arrivals_df['etot_origin_dt_utc'] = arrivals_df['eobt_dt_utc'] + arrivals_df['origin_taxi_out_delta']

    # ELDT at VVTS (Arrivals) = ETOT at Origin + EET from Origin to VVTS
    arrivals_df['eet_to_vvts_delta'] = pd.to_timedelta(arrivals_df['origin_eet_to_vvts_minutes'], unit='m', errors='coerce')
    arrivals_df['eldt_dt_utc'] = arrivals_df['etot_origin_dt_utc'] + arrivals_df['eet_to_vvts_delta'] # Corrected ELDT formula from ETOT + EET

    # EIBT at VVTS for Arrivals = ELDT at VVTS + Taxi-in at VVTS
    arrivals_df['eibt_vvts_dt_utc'] = arrivals_df['eldt_dt_utc'] + pd.to_timedelta(arrivals_df['dest_taxi_in_minutes'], unit='m', errors='coerce') # dest_taxi_in_minutes is VVTS taxi-in here


    # Xử lý chuyến đi (Departures)
    departures_df = flights_df[flights_df['origin'] == 'VVTS'].copy()

    # ETOT at VVTS (Departures) = EOBT at VVTS + Taxi-out from VVTS
    departures_df['etot_dt_utc'] = departures_df['eobt_dt_utc'] + pd.to_timedelta(VVTS_CONFIG['taxi_out_time_minutes'], unit='m', errors='coerce') # VVTS_CONFIG['taxi_out_time_minutes'] is STT for VVTS

    # ELDT at Destination for Departures = ETOT at VVTS + EET from VVTS to Destination
    departures_df['eet_from_vvts_delta'] = pd.to_timedelta(departures_df['dest_eet_from_vvts_minutes'], unit='m', errors='coerce') # Use dest EET from VVTS
    departures_df['eldt_at_dest_dt_utc'] = departures_df['etot_dt_utc'] + departures_df['eet_from_vvts_delta']

    # EIBT at Destination for Departures = ELDT at Destination + Taxi-in at Destination
    departures_df['eibt_at_dest_dt_utc'] = departures_df['eldt_at_dest_dt_utc'] + pd.to_timedelta(departures_df['dest_taxi_in_minutes'], unit='m', errors='coerce')

    return arrivals_df, departures_df

# Hàm tạo Pre-tactical Demand Data
def generate_pre_tactical_demand_data(all_initial_traffic_df):
    """
    Tạo dữ liệu nhu cầu tiền chiến thuật bằng cách áp dụng độ trễ/biến động ngẫu nhiên.
    Args:
        all_initial_traffic_df (pd.DataFrame): DataFrame chứa tất cả các chuyến bay ban đầu (đã được tính toán ELDT/ETOT gốc).
    Returns:
        pd.DataFrame: DataFrame mới với các cột thời gian dự đoán tiền chiến thuật.
    """
    pre_tactical_df = all_initial_traffic_df.copy()

    # Các thông số mô phỏng độ trễ/biến động tiền chiến thuật
    delay_prob_overall = 0.15  # 15% chuyến bay bị ảnh hưởng
    min_delay = 5
    max_delay = 45 # Có thể delay nặng hơn trong Pre-Tactical

    eet_variance_prob = 0.10 # 10% chuyến bay ARR có biến động EET
    max_eet_variance = 20 # Biến động EET lên đến +-20 phút (gió, đường bay)

    pre_tactical_df['is_predicted_delayed'] = False
    pre_tactical_df['prediction_delay_minutes'] = 0.0
    pre_tactical_df['predicted_event_time_utc'] = pre_tactical_df['event_time_utc'] # Khởi tạo bằng thời gian ban đầu

    for idx, row in pre_tactical_df.iterrows():
        current_predicted_time_utc = row['event_time_utc']

        # 1. Áp dụng độ trễ ngẫu nhiên tổng thể (cho cả ARR và DEP)
        if random.random() < delay_prob_overall:
            delay_minutes = random.randint(min_delay, max_delay)
            current_predicted_time_utc += timedelta(minutes=delay_minutes)
            pre_tactical_df.loc[idx, 'is_predicted_delayed'] = True
            pre_tactical_df.loc[idx, 'prediction_delay_minutes'] = delay_minutes

        # 2. Áp dụng biến động EET (chủ yếu ảnh hưởng Arrival ELDT)
        if row['flight_type'] == 'arrival' and random.random() < eet_variance_prob:
            eet_change_minutes = random.randint(-max_eet_variance, max_eet_variance)
            current_predicted_time_utc += timedelta(minutes=eet_change_minutes)
            # Cộng dồn vào prediction_delay_minutes
            pre_tactical_df.loc[idx, 'prediction_delay_minutes'] += eet_change_minutes

        pre_tactical_df.loc[idx, 'predicted_event_time_utc'] = current_predicted_time_utc

    # Tính toán các cột hiển thị thời gian Local cho dữ liệu Pre-Tactical
    pre_tactical_df['predicted_event_time_local'] = pre_tactical_df['predicted_event_time_utc'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])

    return pre_tactical_df

def run_gdp_simulation_for_all_traffic(initial_all_traffic_df, takeoff_capacity_hourly, landing_capacity_hourly, reduced_capacity_events):
    """
    Thực hiện mô phỏng GDP với thuật toán được thiết kế lại:
    Ưu tiên 1: Kiểm tra năng lực theo giờ.
    Ưu tiên 2: Kiểm tra khoảng cách theo phút.
    """
    all_traffic = initial_all_traffic_df.copy()
    all_traffic['regulated_time_utc'] = pd.NaT
    all_traffic['atfm_delay_minutes'] = 0.0
    all_traffic['is_regulated'] = False

    allocated_slots = []
    
    # Sắp xếp các chuyến bay theo thời gian dự kiến để xử lý
    all_traffic = all_traffic.sort_values(by='predicted_event_time_utc').reset_index(drop=True)

    for idx, flight in all_traffic.iterrows():
        desired_time = flight['predicted_event_time_utc']
        if pd.isna(desired_time):
            st.warning(f"Bỏ qua chuyến bay {flight.get('callsign', 'N/A')} do thiếu thời gian dự đoán.")
            continue

        found_slot = False
        # Bắt đầu tìm kiếm từ thời gian mong muốn của chuyến bay
        check_time = desired_time
        
        # Thêm điều kiện dừng để tránh lặp vô hạn
        loop_limiter = 0
        max_loops = 50000 

        while not found_slot:
            loop_limiter += 1
            if loop_limiter > max_loops:
                st.error(f"Lỗi: Vòng lặp vô hạn khi tìm slot cho chuyến bay {flight.get('callsign', 'N/A')}. Gán thời gian gốc và tiếp tục.")
                all_traffic.loc[idx, 'regulated_time_utc'] = desired_time
                break
            
            # --- BƯỚC 1: KIỂM TRA NĂNG LỰC CỦA CẢ GIỜ ---
            current_hour = check_time.replace(minute=0, second=0, microsecond=0)
            
            # Lấy năng lực hiệu dụng cho giờ này
            effective_capacity_total = takeoff_capacity_hourly + landing_capacity_hourly
            for event in reduced_capacity_events:
                if event['start_time_utc'] <= current_hour < event['end_time_utc']:
                    effective_capacity_total = min(effective_capacity_total, event['new_capacity'])
                    break
            
            # Đếm số slot đã được cấp trong giờ này
            demand_in_hour = sum(1 for slot in allocated_slots if slot.date() == current_hour.date() and slot.hour == current_hour.hour)

            if demand_in_hour >= effective_capacity_total:
                # NẾU GIỜ ĐÃ ĐẦY -> NHẢY ĐẾN ĐẦU GIỜ TIẾP THEO và lặp lại
                check_time = current_hour + timedelta(hours=1)
                continue

            # --- BƯỚC 2: NẾU GIỜ CÒN CHỖ, KIỂM TRA KHOẢNG CÁCH PHÚT ---
            is_separation_ok = True
            for existing_slot in allocated_slots:
                time_diff = abs((check_time - existing_slot).total_seconds() / 60)
                if time_diff < VVTS_CONFIG['min_separation_minutes']:
                    is_separation_ok = False
                    break
            
            if is_separation_ok:
                # ĐÃ TÌM THẤY SLOT HỢP LỆ!
                final_slot_time = check_time
                
                # Gán slot và tính toán độ trễ
                all_traffic.loc[idx, 'regulated_time_utc'] = final_slot_time
                delay = (final_slot_time - desired_time).total_seconds() / 60
                if delay > 0.1: # Coi như trễ nếu lớn hơn vài giây
                    all_traffic.loc[idx, 'atfm_delay_minutes'] = delay
                    all_traffic.loc[idx, 'is_regulated'] = True
                
                allocated_slots.append(final_slot_time)
                allocated_slots.sort()
                found_slot = True # Thoát khỏi vòng lặp while
            else:
                # Nếu phút này không được, thử phút tiếp theo
                check_time += timedelta(minutes=1)
    
    # ----- PHẦN CODE CÒN LẠI CỦA HÀM GIỮ NGUYÊN -----
    df_result_with_display_cols = all_traffic.copy()

    df_result_with_display_cols['regulated_time_local'] = df_result_with_display_cols['regulated_time_utc'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])
    df_result_with_display_cols['original_event_time_local'] = df_result_with_display_cols['original_event_time_utc'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])
    df_result_with_display_cols['eobt_dt_local_display'] = df_result_with_display_cols['eobt_dt_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

    def calculate_ctot_utc(row):
        if pd.isna(row['regulated_time_utc']):
            return pd.NaT
        if row['flight_type'] == 'arrival':
            eet_delta = pd.to_timedelta(row['origin_eet_to_vvts_minutes'], unit='m', errors='coerce')
            if pd.isna(eet_delta):
                eet_delta = timedelta(0)
            return row['regulated_time_utc'] - eet_delta
        else:
            return row['regulated_time_utc']

    df_result_with_display_cols['ctot_utc'] = df_result_with_display_cols.apply(calculate_ctot_utc, axis=1)
    tz_offset = timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])
    df_result_with_display_cols['ctot_new_local'] = (df_result_with_display_cols['ctot_utc'] + tz_offset).dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    df_result_with_display_cols.drop(columns=['ctot_utc'], inplace=True)

    df_result_with_display_cols['new_scheduled_time_local'] = df_result_with_display_cols['regulated_time_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    df_result_with_display_cols['original_scheduled_time_local'] = df_result_with_display_cols['original_event_time_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

    final_schema_cols = get_empty_display_dataframe_schema().columns
    for col in final_schema_cols:
        if col not in df_result_with_display_cols.columns:
            df_result_with_display_cols[col] = pd.NA
    
    return df_result_with_display_cols

# --- HÀM MỚI: MÔ PHỎNG SỰ TUÂN THỦ CTOT TRONG THỰC TẾ ---
def simulate_ctot_compliance(regulated_df):
    """
    Mô phỏng sự tuân thủ CTOT với dung sai -5/+10 phút.
    Hàm này lấy DataFrame đã được điều tiết và thêm vào một cột 'actual_time_utc'
    để phản ánh thời gian cất/hạ cánh thực tế có tính đến sự linh hoạt.
    """
    st.info("Bước cuối: Mô phỏng sự tuân thủ CTOT với dung sai -5/+10 phút...")
    
    df_with_actuals = regulated_df.copy()
    
    # Tạo cột mới để chứa thời gian thực tế
    df_with_actuals['actual_time_utc'] = df_with_actuals['regulated_time_utc']
    
    # Chỉ áp dụng cho các chuyến bay bị điều tiết
    regulated_indices = df_with_actuals[df_with_actuals['is_regulated'] == True].index
    
    for index in regulated_indices:
        # Tạo độ lệch ngẫu nhiên từ -5 đến +10 phút
        compliance_offset = random.randint(-5, 10)
        
        # Cập nhật thời gian thực tế
        df_with_actuals.loc[index, 'actual_time_utc'] = df_with_actuals.loc[index, 'regulated_time_utc'] + timedelta(minutes=compliance_offset)
        
    return df_with_actuals

# --- CẢI TIẾN: Thuật toán GDP 2 bước (Dual-Pass) phiên bản CUỐI CÙNG, SỬA LỖI MẤT DỮ LIỆU ---
def run_dual_pass_gdp_simulation(pre_tactical_df, takeoff_capacity, landing_capacity, capacity_events, timezone_offset_hours):
    """
    Thực hiện mô phỏng GDP 2 bước, phiên bản cuối cùng:
    - Sửa lỗi mất dữ liệu của các chuyến bay không bị điều tiết.
    - Đảm bảo tất cả chuyến bay đều có trong kết quả cuối cùng.
    """
    st.info("Bắt đầu quy trình điều tiết 2 bước (phiên bản cuối cùng)...")
    
    arrivals_df = pre_tactical_df[pre_tactical_df['flight_type'] == 'arrival'].copy()
    departures_df = pre_tactical_df[pre_tactical_df['flight_type'] == 'departure'].copy()

    # ==============================================================================
    # ===== BƯỚC A: ĐIỀU TIẾT LUỒNG ĐẾN (ARRIVAL PASS) =============================
    # ==============================================================================
    st.info("Bước A: Điều tiết các chuyến bay hạ cánh (Arrivals)...")
    
    regulated_arrivals_df = arrivals_df.copy() # Khởi tạo df kết quả cho arrival
    
    if not arrivals_df.empty and arrivals_df['predicted_event_time_local'].notna().any():
        hourly_arrival_demand = arrivals_df.groupby(arrivals_df['predicted_event_time_local'].dt.floor('H')).size()
        demand_df = pd.DataFrame(index=hourly_arrival_demand.index)
        demand_df['predicted_demand'] = hourly_arrival_demand
        demand_df['capacity'] = landing_capacity
        congested_arrival_hours = demand_df[demand_df['predicted_demand'] > demand_df['capacity']].index

        if not congested_arrival_hours.empty:
            st.warning(f"Phát hiện {len(congested_arrival_hours)} giờ tắc nghẽn hạ cánh.")
            arr_reg_start_utc = (congested_arrival_hours.min() - timedelta(hours=timezone_offset_hours))
            arr_reg_end_utc = (congested_arrival_hours.max() + timedelta(hours=1) - timedelta(hours=timezone_offset_hours))
            
            arrivals_to_regulate = arrivals_df[(arrivals_df['predicted_event_time_utc'] >= arr_reg_start_utc) & (arrivals_df['predicted_event_time_utc'] < arr_reg_end_utc)].copy()
            arrivals_not_regulated = arrivals_df.drop(arrivals_to_regulate.index)
            
            arrivals_to_regulate.sort_values(by='predicted_event_time_utc', inplace=True)
            slot_interval_minutes = 60 / landing_capacity
            current_slot_time_utc = arr_reg_start_utc
            
            for index, flight in arrivals_to_regulate.iterrows():
                new_slot_time = max(current_slot_time_utc, flight['predicted_event_time_utc'])
                arrivals_to_regulate.loc[index, 'regulated_time_utc'] = new_slot_time
                current_slot_time_utc = new_slot_time + timedelta(minutes=slot_interval_minutes)
            
            # --- SỬA LỖI: Gán thời gian cho các chuyến KHÔNG bị điều tiết ---
            arrivals_not_regulated['regulated_time_utc'] = arrivals_not_regulated['predicted_event_time_utc']
            
            regulated_arrivals_df = pd.concat([arrivals_to_regulate, arrivals_not_regulated])
        else:
            st.success("Luồng hạ cánh thông thoáng.")
            regulated_arrivals_df['regulated_time_utc'] = regulated_arrivals_df['predicted_event_time_utc']
    else:
        st.success("Không có chuyến bay hạ cánh nào trong dữ liệu.")
    
    # ==============================================================================
    # ===== BƯỚC B: ĐIỀU TIẾT LUỒNG ĐI (DEPARTURE PASS) ============================
    # ==============================================================================
    st.info("Bước B: Điều tiết các chuyến bay cất cánh (Departures)...")
    
    regulated_departures_df = departures_df.copy() # Khởi tạo df kết quả cho departure

    # Tính toán năng lực còn lại...
    total_capacity = takeoff_capacity + landing_capacity
    regulated_arrivals_df['regulated_time_local'] = regulated_arrivals_df['regulated_time_utc'] + timedelta(hours=timezone_offset_hours)
    regulated_arrivals_per_hour = regulated_arrivals_df.groupby(regulated_arrivals_df['regulated_time_local'].dt.floor('H')).size()
    
    if not departures_df.empty and departures_df['predicted_event_time_local'].notna().any():
        departures_per_hour = departures_df.groupby(departures_df['predicted_event_time_local'].dt.floor('H')).size()
        
        start_hour_range = min(regulated_arrivals_df['regulated_time_local'].min(), departures_df['predicted_event_time_local'].min()) if not regulated_arrivals_df.empty else departures_df['predicted_event_time_local'].min()
        end_hour_range = max(regulated_arrivals_df['regulated_time_local'].max(), departures_df['predicted_event_time_local'].max()) if not regulated_arrivals_df.empty else departures_df['predicted_event_time_local'].max()

        full_day_hours = pd.date_range(start=start_hour_range.floor('H'), end=end_hour_range.floor('H'), freq='H')
        
        hourly_summary = pd.DataFrame(index=full_day_hours)
        hourly_summary['regulated_arrivals'] = regulated_arrivals_per_hour.reindex(full_day_hours, fill_value=0)
        hourly_summary['predicted_departures'] = departures_per_hour.reindex(full_day_hours, fill_value=0)
        hourly_summary['departure_capacity'] = total_capacity - hourly_summary['regulated_arrivals']
        hourly_summary['departure_capacity'] = hourly_summary['departure_capacity'].apply(lambda x: max(takeoff_capacity/4, x))
        
        congested_departure_hours = hourly_summary[hourly_summary['predicted_departures'] > hourly_summary['departure_capacity']].index
        
        if not congested_departure_hours.empty:
            st.warning(f"Phát hiện {len(congested_departure_hours)} giờ tắc nghẽn cất cánh.")
            
            dep_reg_start_utc = (congested_departure_hours.min() - timedelta(hours=timezone_offset_hours))
            
            departures_to_regulate = departures_df[departures_df['predicted_event_time_utc'] >= dep_reg_start_utc].copy()
            departures_not_regulated = departures_df.drop(departures_to_regulate.index)
            
            departures_to_regulate.sort_values(by='predicted_event_time_utc', inplace=True)
            
            current_slot_time_utc = dep_reg_start_utc
            for index, flight in departures_to_regulate.iterrows():
                new_slot_time = max(current_slot_time_utc, flight['predicted_event_time_utc'])
                slot_hour_local = (new_slot_time + timedelta(hours=timezone_offset_hours)).floor('H')
                dep_capacity_this_hour = hourly_summary.loc[slot_hour_local, 'departure_capacity'] if slot_hour_local in hourly_summary.index else takeoff_capacity
                slot_interval_minutes = 60 / dep_capacity_this_hour
                departures_to_regulate.loc[index, 'regulated_time_utc'] = new_slot_time
                current_slot_time_utc = new_slot_time + timedelta(minutes=slot_interval_minutes)

            # --- SỬA LỖI: Gán thời gian cho các chuyến KHÔNG bị điều tiết ---
            departures_not_regulated['regulated_time_utc'] = departures_not_regulated['predicted_event_time_utc']

            regulated_departures_df = pd.concat([departures_to_regulate, departures_not_regulated])
        else:
            st.success("Luồng cất cánh thông thoáng.")
            regulated_departures_df['regulated_time_utc'] = regulated_departures_df['predicted_event_time_utc']
    else:
        st.success("Không có chuyến bay cất cánh nào trong dữ liệu.")
    
    # ==============================================================================
    # ===== BƯỚC C: KẾT HỢP VÀ HOÀN THIỆN ==========================================
    # ==============================================================================
    final_df = pd.concat([regulated_arrivals_df, regulated_departures_df]).dropna(subset=['callsign'])
    
    # Phần tính toán cuối cùng không thay đổi
    # ... (toàn bộ phần code tính toán các cột cuối cùng và return giữ nguyên)
    final_df['atfm_delay_minutes'] = (final_df['regulated_time_utc'] - final_df['predicted_event_time_utc']).dt.total_seconds() / 60
    final_df['atfm_delay_minutes'] = final_df['atfm_delay_minutes'].apply(lambda x: max(0, x))
    final_df['is_regulated'] = final_df['atfm_delay_minutes'] > 0.1

    df_result_with_display_cols = final_df.copy()
    df_result_with_display_cols['regulated_time_local'] = pd.to_datetime(df_result_with_display_cols['regulated_time_utc']).dt.tz_localize('UTC').dt.tz_convert(f'Etc/GMT-{timezone_offset_hours}').dt.tz_localize(None)
    df_result_with_display_cols['original_event_time_local'] = pd.to_datetime(df_result_with_display_cols['original_event_time_utc']).dt.tz_localize('UTC').dt.tz_convert(f'Etc/GMT-{timezone_offset_hours}').dt.tz_localize(None)
    
    def calculate_ctot_utc(row):
        if pd.isna(row['regulated_time_utc']) or not row['is_regulated']: return pd.NaT
        if row['flight_type'] == 'arrival':
            eet_delta = pd.to_timedelta(row['origin_eet_to_vvts_minutes'], unit='m')
            return row['regulated_time_utc'] - eet_delta
        else: return row['regulated_time_utc']

    df_result_with_display_cols['ctot_utc'] = df_result_with_display_cols.apply(calculate_ctot_utc, axis=1)
    df_result_with_display_cols['ctot_new_local'] = pd.to_datetime(df_result_with_display_cols['ctot_utc']).dt.tz_localize('UTC').dt.tz_convert(f'Etc/GMT-{timezone_offset_hours}').dt.tz_localize(None).dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    df_result_with_display_cols['new_scheduled_time_local'] = df_result_with_display_cols['regulated_time_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    df_result_with_display_cols['original_scheduled_time_local'] = pd.to_datetime(df_result_with_display_cols['original_event_time_local']).dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

    final_schema_cols = get_empty_display_dataframe_schema().columns
    for col in final_schema_cols:
        if col not in df_result_with_display_cols.columns:
            df_result_with_display_cols[col] = pd.NA
    
    st.success("Hoàn tất mô phỏng điều tiết!")
    return df_result_with_display_cols[list(final_schema_cols)]
def run_selective_gdp_simulation(pre_tactical_df, takeoff_capacity, landing_capacity, capacity_events, timezone_offset_hours):
    """
    Điều phối việc chạy GDP bằng cách xác định các giờ tắc nghẽn trước,
    sau đó chỉ chạy mô phỏng trên các chuyến bay bị ảnh hưởng.
    """
    st.info("Bắt đầu quy trình điều tiết có chọn lọc...")

    # --- Bước 1: Xác định các khung giờ tắc nghẽn (dựa trên luồng hạ cánh) ---
    # Sử dụng dữ liệu dự báo tiền chiến thuật để xác định tắc nghẽn
    arrivals_predicted = pre_tactical_df[pre_tactical_df['flight_type'] == 'arrival'].copy()
    if 'predicted_event_time_local' not in arrivals_predicted.columns:
         arrivals_predicted['predicted_event_time_local'] = arrivals_predicted['predicted_event_time_utc'] + timedelta(hours=timezone_offset_hours)

    hourly_predicted_demand = arrivals_predicted.groupby(arrivals_predicted['predicted_event_time_local'].dt.floor('H')).size()
    
    # Tạo một DataFrame đầy đủ 24h để so sánh
    first_hour = hourly_predicted_demand.index.min().floor('H')
    last_hour = hourly_predicted_demand.index.max().floor('H')
    full_day_hours = pd.date_range(start=first_hour, end=last_hour, freq='H')
    demand_df = pd.DataFrame(index=full_day_hours)
    demand_df['predicted_demand'] = hourly_predicted_demand.reindex(demand_df.index, fill_value=0)
    demand_df['capacity'] = landing_capacity

    # Xác định các giờ tắc nghẽn
    congested_hours_local = demand_df[demand_df['predicted_demand'] > demand_df['capacity']].index
    
    if congested_hours_local.empty:
        st.success("Phân tích nhu cầu dự báo: Không phát hiện khung giờ tắc nghẽn nào. Không cần áp dụng GDP.")
        # Trả về dữ liệu gốc với các cột điều tiết được thêm vào và để trống
        result_df = pre_tactical_df.copy()
        result_df['is_regulated'] = False
        result_df['atfm_delay_minutes'] = 0.0
        result_df['regulated_time_utc'] = result_df['predicted_event_time_utc']
        result_df['regulated_time_local'] = result_df['predicted_event_time_local']
        result_df['new_scheduled_time_local'] = result_df['regulated_time_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        result_df['ctot_new_local'] = '' # Không có CTOT mới
        return result_df
    
    st.warning(f"Phát hiện {len(congested_hours_local)} khung giờ có nguy cơ tắc nghẽn (landing). Bắt đầu lọc chuyến bay để điều tiết.")
    st.write("Các khung giờ tắc nghẽn (giờ địa phương):", [t.strftime('%Y-%m-%d %H:%M') for t in congested_hours_local])

    # --- Bước 2: Lọc chuyến bay cần điều tiết ---
    # Chuyển giờ tắc nghẽn về UTC để lọc
    congested_start_utc = congested_hours_local.min() - timedelta(hours=timezone_offset_hours)
    congested_end_utc = congested_hours_local.max() + timedelta(hours=1) - timedelta(hours=timezone_offset_hours)

    # Lọc tất cả các chuyến bay (cả đến và đi) trong toàn bộ cửa sổ tắc nghẽn
    # Điều này đảm bảo rằng các chuyến bay đi cũng được xem xét trong năng lực tổng thể
    flights_to_regulate = pre_tactical_df[
        (pre_tactical_df['predicted_event_time_utc'] >= congested_start_utc) &
        (pre_tactical_df['predicted_event_time_utc'] < congested_end_utc)
    ].copy()

    # Các chuyến bay không cần điều tiết
    non_regulated_flights = pre_tactical_df.drop(flights_to_regulate.index).copy()

    st.info(f"Đã lọc được {len(flights_to_regulate)} chuyến bay để đưa vào mô phỏng GDP. {len(non_regulated_flights)} chuyến bay còn lại hoạt động bình thường.")

    # --- Bước 3: Chạy GDP có chọn lọc ---
    with st.spinner(f"Đang chạy mô phỏng GDP cho {len(flights_to_regulate)} chuyến bay trong vùng tắc nghẽn..."):
        regulated_flights_df = run_gdp_simulation_for_all_traffic(
            flights_to_regulate,
            takeoff_capacity,
            landing_capacity,
            capacity_events
        )
    with st.spinner("Đang chạy mô phỏng GDP..."):
                # Bước 1: Chạy GDP để có lịch trình lý tưởng
                ideal_regulated_data = run_dual_pass_gdp_simulation(
                    st.session_state.pre_tactical_demand_data,
                    st.session_state.takeoff_capacity,
                    st.session_state.landing_capacity,
                    st.session_state.reduced_capacity_events,
                    VVTS_CONFIG['airport_timezone_offset_hours']
                )
                
                # --- THÊM MỚI: Áp dụng dung sai tuân thủ ---
                # Bước 2: Mô phỏng sự tuân thủ trong thực tế
                st.session_state.regulated_flights_data = simulate_ctot_compliance(ideal_regulated_data)
                st.session_state.simulation_run = True
    # --- Bước 4: Kết hợp kết quả ---
    # Chuẩn bị dữ liệu cho các chuyến không bị điều tiết
    non_regulated_flights['is_regulated'] = False
    non_regulated_flights['atfm_delay_minutes'] = 0.0
    non_regulated_flights['regulated_time_utc'] = non_regulated_flights['predicted_event_time_utc']
    non_regulated_flights['regulated_time_local'] = non_regulated_flights['predicted_event_time_local']
    # Đảm bảo các cột hiển thị tồn tại và đúng định dạng
    non_regulated_flights['new_scheduled_time_local'] = non_regulated_flights['regulated_time_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    non_regulated_flights['ctot_new_local'] = '' # Không có CTOT mới

    # Kết hợp hai bảng dữ liệu
    final_df = pd.concat([regulated_flights_df, non_regulated_flights], ignore_index=True)
    
    # Sắp xếp lại theo thời gian đã điều tiết để hiển thị hợp lý
    final_df.sort_values(by='regulated_time_utc', inplace=True)
    
    st.success("Hoàn tất mô phỏng điều tiết có chọn lọc!")
    return final_df
# --- Giao diện Streamlit ---

# Khởi tạo trạng thái session nếu chưa có
if 'simulation_run' not in st.session_state:
    st.session_state.simulation_run = False
if 'regulated_flights_data' not in st.session_state:
    st.session_state.regulated_flights_data = get_empty_display_dataframe_schema()
if 'pre_tactical_demand_data' not in st.session_state:
    st.session_state.pre_tactical_demand_data = get_empty_display_dataframe_schema()

if 'initial_arrivals' not in st.session_state:
    st.session_state.initial_arrivals = pd.DataFrame()
if 'initial_departures' not in st.session_state:
    st.session_state.initial_departures = pd.DataFrame()
if 'takeoff_capacity' not in st.session_state:
    st.session_state.takeoff_capacity = VVTS_CONFIG['takeoff_capacity_hourly']
if 'landing_capacity' not in st.session_state:
    st.session_state.landing_capacity = VVTS_CONFIG['landing_capacity_hourly']
if 'reduced_capacity_events' not in st.session_state:
    st.session_state.reduced_capacity_events = []
if 'selected_date' not in st.session_state:
    st.session_state.selected_date = datetime.utcnow().date()

st.title("ATFM Simulation Dashboard - Sân bay Quốc tế Tân Sơn Nhất (VVTS)")

# --- Thông tin tổng quan ---
st.info(f"Dashboard này mô phỏng Quản lý luồng không lưu (ATFM) tại Sân bay Quốc tế Tân Sơn Nhất (VVTS), múi giờ **UTC+{VVTS_CONFIG['airport_timezone_offset_hours']} (Giờ địa phương Việt Nam)**.")
st.markdown("---")

# --- Sidebar cho Cấu hình Mô phỏng ---
st.sidebar.header("Cấu hình Mô phỏng")
st.sidebar.markdown("---")

# Tùy chọn ngày mô phỏng
st.sidebar.subheader("Chọn Ngày Mô phỏng")
temp_flights_df, temp_eets_df = load_data()
if temp_flights_df is not None and not temp_flights_df.empty:
    min_date_data = temp_flights_df['eobt_dt_local'].dt.date.min()
    max_date_data = temp_flights_df['eobt_dt_local'].dt.date.max()

    default_date_picker = st.session_state.selected_date
    if not (min_date_data <= default_date_picker <= max_date_data):
        default_date_picker = min_date_data

    st.session_state.selected_date = st.sidebar.date_input(
        "Chọn ngày để xem dữ liệu:",
        value=default_date_picker,
        min_value=min_date_data,
        max_value=max_date_data,
        key="simulation_date_picker"
    )
    flights_df_for_selected_date = temp_flights_df[temp_flights_df['eobt_dt_local'].dt.date == st.session_state.selected_date].copy()
    eets_df = temp_eets_df
else:
    st.error("Không thể tải dữ liệu chuyến bay hoặc dữ liệu trống. Vui lòng kiểm tra file CSV và chạy lại.")
    st.stop()


st.sidebar.markdown("---")

# Tùy chỉnh Năng lực Sân bay (cất và hạ riêng)
st.sidebar.subheader("Năng lực Sân bay")
st.session_state.takeoff_capacity = st.sidebar.number_input(
    "Năng lực Cất cánh (lượt/giờ):",
    min_value=0,
    max_value=100,
    value=st.session_state.takeoff_capacity,
    key="takeoff_cap_input"
)
st.session_state.landing_capacity = st.sidebar.number_input(
    "Năng lực Hạ cánh (lượt/giờ):",
    min_value=0,
    max_value=100,
    value=st.session_state.landing_capacity,
    key="landing_cap_input"
)

if st.sidebar.button("Đặt lại Năng lực Mặc định"):
    st.session_state.takeoff_capacity = VVTS_CONFIG['takeoff_capacity_hourly']
    st.session_state.landing_capacity = VVTS_CONFIG['landing_capacity_hourly']
    st.rerun()

st.sidebar.markdown("---")

# --- Kịch bản Giảm Năng lực ---
st.sidebar.subheader("Kịch bản Giảm Năng lực (Sự kiện bất thường)")
st.sidebar.info("Năng lực được định nghĩa dưới đây sẽ ghi đè năng lực tổng cộng (Hạ cánh + Cất cánh) nếu xảy ra trong khoảng thời gian của sự kiện. **Ảnh hưởng đến GDP cho toàn bộ lưu lượng.**")

with st.sidebar.expander("Thêm Sự kiện Giảm Năng lực"):
    event_start_date = st.date_input("Ngày bắt đầu:", st.session_state.selected_date, key="event_start_date")
    event_start_time = st.time_input("Giờ bắt đầu:", time(9, 0), key="event_start_time")
    event_end_date = st.date_input("Ngày kết thúc:", st.session_state.selected_date, key="event_end_date")
    event_end_time = st.time_input("Giờ kết thúc:", time(12, 0), key="event_end_time")

    max_event_capacity_total = VVTS_CONFIG['takeoff_capacity_hourly'] + VVTS_CONFIG['landing_capacity_hourly']
    event_new_capacity = st.number_input("Năng lực mới (tổng lượt/giờ):", min_value=0, max_value=max_event_capacity_total, value=min(30, max_event_capacity_total), key="event_new_capacity")

    if st.button("Thêm Sự kiện"):
        start_dt_local = datetime.combine(event_start_date, event_start_time)
        end_dt_local = datetime.combine(event_end_date, event_end_time)

        if start_dt_local >= end_dt_local:
            st.error("Thời gian kết thúc phải sau thời gian bắt đầu.")
        else:
            event_data = {
                'start_time_local': start_dt_local,
                'end_time_local': end_dt_local,
                'start_time_utc': start_dt_local - timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours']),
                'end_time_utc': end_dt_local - timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours']),
                'new_capacity': event_new_capacity
            }
            st.session_state.reduced_capacity_events.append(event_data)
            st.success("Đã thêm sự kiện giảm năng lực.")
            st.rerun()


if st.session_state.reduced_capacity_events:
    st.sidebar.markdown("##### Danh sách Sự kiện đã thêm:")
    for i, event in enumerate(st.session_state.reduced_capacity_events):
        st.sidebar.write(f"- Sự kiện {i+1}: Từ {event['start_time_local'].strftime('%H:%M %d/%m')} đến {event['end_time_local'].strftime('%H:%M %d/%m')} - Năng lực: {event['new_capacity']}")
    if st.sidebar.button("Xóa tất cả Sự kiện Giảm Năng lực"):
        st.session_state.reduced_capacity_events = []
        st.rerun()
else:
    st.sidebar.info("Chưa có sự kiện giảm năng lực nào được thêm.")

st.sidebar.markdown("---")

# Nút Reset Dashboard hoàn toàn
if st.sidebar.button("Reset Dashboard (Bắt đầu lại)"):
    for key in st.session_state.keys():
        del st.session_state[key]
    st.rerun()

# Tính lịch trình ban đầu cho ngày được chọn
initial_arrivals_df, initial_departures_df = calculate_initial_schedules(flights_df_for_selected_date, eets_df)
st.session_state.initial_arrivals = initial_arrivals_df
st.session_state.initial_departures = initial_departures_df


# --- Phần Chính của Dashboard ---

# --- Định nghĩa các Tabs ---
tab_demand_strategic, tab_pre_tactical, tab_gdp = st.tabs(["Demand & Strategic Data", "Pre-tactical Demand", "Tactical (GDP) Simulation Results"])

with tab_demand_strategic: # Nội dung Tab 1
    st.header(f"Air Traffic Demand on VVTS (Ngày {st.session_state.selected_date.strftime('%d/%m/%Y')})")

    # Chuyển đổi thời gian về múi giờ địa phương để hiển thị trên biểu đồ
    initial_arrivals_df['eldt_dt_local'] = initial_arrivals_df['eldt_dt_utc'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])
    initial_departures_df['etot_dt_local'] = initial_departures_df['etot_dt_utc'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])

    # --- Đảm bảo full_demand_df luôn có đủ 24 giờ của ngày được chọn ---
    selected_date_full_hours = pd.date_range(
        start=datetime.combine(st.session_state.selected_date, time(0,0,0)),
        end=datetime.combine(st.session_state.selected_date, time(23,0,0)),
        freq='H'
    )
    full_demand_df = pd.DataFrame(index=selected_date_full_hours)

    # Tính nhu cầu cất và hạ cánh thực tế theo từng giờ cụ thể
    hourly_arr_demand_series = initial_arrivals_df.groupby(initial_arrivals_df['eldt_dt_local'].dt.floor('H')).size().reindex(full_demand_df.index, fill_value=0)
    hourly_dep_demand_series = initial_departures_df.groupby(initial_departures_df['etot_dt_local'].dt.floor('H')).size().reindex(full_demand_df.index, fill_value=0)

    full_demand_df['arrival_demand'] = hourly_arr_demand_series
    full_demand_df['departure_demand'] = hourly_dep_demand_series


    # Lấy năng lực hạ cánh cơ bản
    full_demand_df['base_landing_capacity'] = st.session_state.landing_capacity
    full_demand_df['base_takeoff_capacity'] = st.session_state.takeoff_capacity

    # Tính tổng năng lực để vẽ đường trên biểu đồ (Năng lực Hạ cánh thực tế + Năng lực Cất cánh)
    full_demand_df['total_effective_capacity'] = full_demand_df['base_landing_capacity'] + full_demand_df['base_takeoff_capacity']

    # Áp dụng các sự kiện giảm năng lực lên TỔNG năng lực
    for event in st.session_state.reduced_capacity_events:
        event_start_local = event['start_time_local']
        event_end_local = event['end_time_local']
        event_capacity = event['new_capacity']

        mask = (full_demand_df.index >= event_start_local) & (full_demand_df.index < event_end_local)
        full_demand_df.loc[mask, 'total_effective_capacity'] = full_demand_df.loc[mask, 'total_effective_capacity'].apply(lambda cap: min(cap, event_capacity))


    # Xác định các điểm quá tải cho luồng hạ cánh (dựa trên nhu cầu hạ cánh so với năng lực hạ cánh)
    full_demand_df['effective_landing_capacity_for_gdp'] = st.session_state.landing_capacity # Dùng riêng cho điều kiện GDP
    full_demand_df['is_landing_overload'] = full_demand_df['arrival_demand'] > full_demand_df['effective_landing_capacity_for_gdp']


    # --- Biểu đồ cột chồng Nhu cầu Ban đầu (Chart 1 - cố định) ---
    st.subheader("Airport Initial Demand")
    fig_initial_stacked_demand = go.Figure()

    # Cột cất cánh (màu xanh dương) - Đặt dưới cùng
    fig_initial_stacked_demand.add_trace(go.Bar(
        x=full_demand_df.index,
        y=full_demand_df['departure_demand'],
        name='Nhu cầu Cất cánh',
        marker_color='blue'
    ))

    # Cột hạ cánh (màu cam) - Chồng lên trên cột cất cánh
    fig_initial_stacked_demand.add_trace(go.Bar(
        x=full_demand_df.index,
        y=full_demand_df['arrival_demand'],
        name='Nhu cầu Hạ cánh',
        marker_color='orange'
    ))

    # Đường năng lực tổng cộng (màu đỏ)
    fig_initial_stacked_demand.add_trace(go.Scatter(
        x=full_demand_df.index,
        y=full_demand_df['total_effective_capacity'],
        mode='lines',
        name='Capacity',
        line=dict(color='red', dash='dash', width=3)
    ))

    fig_initial_stacked_demand.update_layout(
        barmode='stack', # Chế độ cột chồng
        title='Nhu cầu Hoạt động Ban đầu (Đến và Đi) so với Năng lực Sân bay',
        xaxis_title=f'Thời gian (Giờ địa phương - UTC+{VVTS_CONFIG["airport_timezone_offset_hours"]})',
        yaxis_title='Số lượt cất/hạ cánh',
        plot_bgcolor='rgba(0,0,0,0)',
        hovermode="x unified",
        xaxis_tickformat="%H:%M<br>%d/%m"
    )
    st.plotly_chart(fig_initial_stacked_demand, use_container_width=True)

    # Hiển thị cảnh báo quá tải cho luồng hạ cánh
    if full_demand_df['is_landing_overload'].any():
        overload_summary = full_demand_df[full_demand_df['is_landing_overload']]
        st.warning(f"**Phát hiện nguy cơ quá tải năng lực HẠ CÁNH tại {len(overload_summary)} khung giờ.** Vui lòng xem bảng dưới đây để biết chi tiết các giờ quá tải.")
        st.dataframe(overload_summary[['arrival_demand', 'effective_landing_capacity_for_gdp']].rename(columns={'arrival_demand': 'Nhu cầu Hạ cánh', 'effective_landing_capacity_for_gdp': 'Năng lực Hạ cánh Thực tế'}), use_container_width=True)

        # Nút áp dụng GDP được giữ nguyên và bây giờ sẽ được kích hoạt lại
        if st.button("Mô phỏng Ground Delay Programme", key="apply_gdp_button_demand_tab"):
            # Logic GDP sẽ được đặt trong tab_gdp, không chạy ngay tại đây
            st.session_state.should_run_gdp = True # Đặt cờ để kích hoạt GDP khi chuyển tab
            st.session_state.active_tab = "Tactical (GDP) Simulation Results" # Chuyển sang tab GDP
            st.success("Nhấn nút này để chạy GDP.") # Thông báo nhanh
            st.rerun() # Buộc chạy lại để chuyển tab
    else:
        st.success("Không phát hiện nguy cơ quá tải luồng HẠ CÁNH trong ngày đã chọn. Không cần áp dụng GDP.")
        st.button("Áp dụng Chương trình Điều tiết (GDP) cho Toàn bộ Lưu lượng", disabled=True, key="apply_gdp_button_demand_tab_disabled")


    # --- Bổ sung Dữ liệu Chiến lược (Strategic Flight Data) ---
    st.subheader("Strategic Flight Data")

    # Lựa chọn khung giờ cho bảng Strategic Data
    time_options = [f"{h:02d}:00 - {h+1:02d}:00" for h in range(24)]
    time_options.insert(0, "Toàn bộ ngày")
    selected_time_frame = st.selectbox("Chọn khung giờ để hiển thị:", time_options, key="strategic_time_frame")

    # Lọc dữ liệu chuyến bay ban đầu cho mục Strategic Data
    all_initial_traffic = pd.concat([
        initial_arrivals_df.assign(event_time_local=initial_arrivals_df['eldt_dt_local'], movement_type_display='Arrival'),
        initial_departures_df.assign(event_time_local=initial_departures_df['etot_dt_local'], movement_type_display='Departure')
    ])
    all_initial_traffic = all_initial_traffic.sort_values(by='event_time_local').reset_index(drop=True)


    if selected_time_frame == "Toàn bộ ngày":
        strategic_flights_to_display = all_initial_traffic
    else:
        start_hour_str = selected_time_frame.split(' ')[0]
        end_hour_str = selected_time_frame.split(' ')[2]

        start_time_dt = datetime.combine(st.session_state.selected_date, datetime.strptime(start_hour_str, '%H:%M').time())
        end_time_dt = datetime.combine(st.session_state.selected_date, datetime.strptime(end_hour_str, '%H:%M').time())

        strategic_flights_to_display = all_initial_traffic[
            (all_initial_traffic['event_time_local'] >= start_time_dt) &
            (all_initial_traffic['event_time_local'] < end_time_dt)
        ].sort_values(by='event_time_local')

    if not strategic_flights_to_display.empty:
        # --- Departure Flights from VVTS (trong Expander) ---
       # --- Departure Flights from VVTS (trong Expander) ---
        with st.expander("Departures from VVTS"):
            st.subheader("Departure Flights from VVTS")
            
            # Lọc dữ liệu chuyến đi
            dep_strategic_data = strategic_flights_to_display[strategic_flights_to_display['origin'] == 'VVTS'].copy()

            # ---- BẮT ĐẦU KHỐI TÍNH TOÁN BỊ THIẾU ----
            # Tính toán các cột thời gian dạng chuỗi để hiển thị
            dep_strategic_data['EOBT_local_str'] = dep_strategic_data['eobt_dt_local'].dt.strftime('%Y-%m-%d %H:%M:%S')
            dep_strategic_data['ETOT_local_str'] = dep_strategic_data['etot_dt_local'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Tính toán ELDT và EIBT tại sân bay đến
            dep_strategic_data['ELDT_at_dest_dt_local'] = (dep_strategic_data['etot_dt_utc'] + dep_strategic_data['eet_from_vvts_delta'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours']))
            dep_strategic_data['ELDT_local_str'] = dep_strategic_data['ELDT_at_dest_dt_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

            dep_strategic_data['EIBT_at_dest_dt_local'] = (dep_strategic_data['ELDT_at_dest_dt_local'] + pd.to_timedelta(dep_strategic_data['dest_taxi_in_minutes'], unit='m'))
            dep_strategic_data['EIBT_local_str'] = dep_strategic_data['EIBT_at_dest_dt_local'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
            
            # Tạo cột STT (Order)
            dep_strategic_data = dep_strategic_data.reset_index(drop=True)
            dep_strategic_data['Order'] = dep_strategic_data.index + 1
            # ---- KẾT THÚC KHỐI TÍNH TOÁN BỊ THIẾU ----

            # Chọn và hiển thị các cột
            display_cols_dep = ['Order', 'callsign', 'EOBT_local_str', 'ETOT_local_str', 'ELDT_local_str', 'EIBT_local_str', 'destination', 'aircraft_type']
            
            st.dataframe(dep_strategic_data[display_cols_dep].rename(columns={
                'callsign': 'Flight',
                'EOBT_local_str': 'EOBT',
                'ETOT_local_str': 'ETOT',
                'ELDT_local_str': 'ELDT (Dest)',
                'EIBT_local_str': 'EIBT (Dest)',
                'destination': 'Destination Airport',
                'aircraft_type': 'Aircraft Type'
            }), use_container_width=True)

        # --- Arrival Flights to VVTS ---
        # --- Arrival Flights to VVTS (trong Expander) ---
    

    else:
        st.info("Không có dữ liệu chuyến bay cho ngày đã chọn trong khung giờ này.")

with tab_pre_tactical: # Nội dung Tab 2
    st.header(f"Pre-tactical Demand Data Analysis (Ngày {st.session_state.selected_date.strftime('%d/%m/%Y')})")

    # Nút để tạo dữ liệu Pre-tactical (dữ liệu này sẽ được lưu vào session_state)
    if st.button("Tạo Dữ liệu Dự đoán Tiền Chiến thuật (Pre-tactical)", key="generate_pt_data_button"):
        # Initial traffic for pre-tactical generation should come from initial_arrivals_df and initial_departures_df
        # Combine initial arrivals and departures into one DataFrame, ensuring all original columns are carried
        all_initial_traffic_for_pt = pd.concat([
            initial_arrivals_df.assign(event_time_utc=initial_arrivals_df['eldt_dt_utc'], original_event_time_utc=initial_arrivals_df['eldt_dt_utc'], flight_type='arrival'),
            initial_departures_df.assign(event_time_utc=initial_departures_df['etot_dt_utc'], original_event_time_utc=initial_departures_df['etot_dt_utc'], flight_type='departure')
        ])

        # Reindex to ensure all columns from the common initial schema are present
        common_cols_pt = list(set(initial_arrivals_df.columns.tolist() + initial_departures_df.columns.tolist()))
        # Reindex with the union of original columns plus the new assigned columns
        all_initial_traffic_for_pt = all_initial_traffic_for_pt.reindex(columns=list(set(common_cols_pt + ['event_time_utc', 'original_event_time_utc', 'flight_type'])))

        # Fill potential NaN after reindex with appropriate defaults before passing to function
        for col in all_initial_traffic_for_pt.columns:
            if 'dt' in str(all_initial_traffic_for_pt[col].dtype) or 'time' in str(all_initial_traffic_for_pt[col].dtype):
                all_initial_traffic_for_pt[col].fillna(pd.NaT, inplace=True)
            elif 'delta' in str(all_initial_traffic_for_pt[col].dtype):
                all_initial_traffic_for_pt[col].fillna(pd.NaT, inplace=True)
            elif 'float' in str(all_initial_traffic_for_pt[col].dtype):
                all_initial_traffic_for_pt[col].fillna(0.0, inplace=True)
            elif 'bool' in str(all_initial_traffic_for_pt[col].dtype):
                all_initial_traffic_for_pt[col].fillna(False, inplace=True)
            else:
                all_initial_traffic_for_pt[col].fillna('', inplace=True)

        st.session_state.pre_tactical_demand_data = generate_pre_tactical_demand_data(all_initial_traffic_for_pt)
        st.success("Đã tạo dữ liệu dự đoán tiền chiến thuật.")

    # --- SỬA LỖI: TOÀN BỘ LOGIC HIỂN THỊ ĐƯỢC ĐƯA VÀO ĐÂY ---
    # Chỉ hiển thị biểu đồ và các tùy chọn nếu dữ liệu Pre-tactical đã được tạo
    if not st.session_state.pre_tactical_demand_data.empty:
        col_forecast, col_period_pt, col_movement_pt = st.columns(3)

        chart_forecast_type = col_forecast.selectbox("Loại Dự báo:", ["Initial FPL Demand", "Pre-tactical Predicted Demand"], key="pt_forecast_type")
        chart_period_pt = col_period_pt.selectbox("Phạm vi thời gian:", ["1 Hour", "2 Hours", "3 Hours", "6 Hours", "Full Day"], index=4, key="pt_period")
        chart_movement_type_pt = col_movement_pt.selectbox("Loại lưu lượng:", ["Arrival", "Departure", "Total"], index=2, key="pt_movement")

        # Lọc dữ liệu cho biểu đồ Pre-Tactical
        df_for_chart_pt = None
        demand_col = None
        if chart_forecast_type == "Initial FPL Demand":
            df_for_chart_pt = pd.concat([
                initial_arrivals_df.assign(chart_event_time_local=initial_arrivals_df['eldt_dt_local'], flight_type='arrival'),
                initial_departures_df.assign(chart_event_time_local=initial_departures_df['etot_dt_local'], flight_type='departure')
            ]).sort_values(by='chart_event_time_local')
            demand_col = 'chart_event_time_local'
        else: # Pre-tactical Predicted Demand
            df_for_chart_pt = st.session_state.pre_tactical_demand_data.copy()
            demand_col = 'predicted_event_time_local'

        # Tính toán nhu cầu dựa trên lựa chọn Movement Type
        hourly_demand_pt = None
        chart_yaxis_title_pt = ""
        chart_marker_color_pt = ''
        chart_capacity_value_pt = None
        chart_capacity_name_pt = ""

        if demand_col in df_for_chart_pt.columns and not df_for_chart_pt[demand_col].isnull().all():
            if chart_movement_type_pt == "Arrival":
                hourly_demand_pt = df_for_chart_pt[df_for_chart_pt['flight_type'] == 'arrival'].groupby(df_for_chart_pt[demand_col].dt.floor('H')).size()
                chart_yaxis_title_pt = "Số lượt hạ cánh"
                chart_marker_color_pt = 'orange'
                chart_capacity_value_pt = full_demand_df['base_landing_capacity']
                chart_capacity_name_pt = "Năng lực Hạ cánh"
            elif chart_movement_type_pt == "Departure":
                hourly_demand_pt = df_for_chart_pt[df_for_chart_pt['flight_type'] == 'departure'].groupby(df_for_chart_pt[demand_col].dt.floor('H')).size()
                chart_yaxis_title_pt = "Số lượt cất cánh"
                chart_marker_color_pt = 'blue'
                chart_capacity_value_pt = full_demand_df['base_takeoff_capacity']
                chart_capacity_name_pt = "Năng lực Cất cánh"
            else: # Total
                hourly_demand_pt = df_for_chart_pt.groupby(df_for_chart_pt[demand_col].dt.floor('H')).size()
                chart_yaxis_title_pt = "Số lượt cất/hạ cánh"
                chart_marker_color_pt = 'cornflowerblue'
                chart_capacity_value_pt = full_demand_df['total_effective_capacity']
                chart_capacity_name_pt = "Năng lực Tổng cộng"

            hourly_demand_pt = hourly_demand_pt.reindex(full_demand_df.index, fill_value=0)
        else:
            st.warning(f"Không có dữ liệu hợp lệ cho '{chart_movement_type_pt}' trong loại dự báo '{chart_forecast_type}'. Vui lòng tạo dữ liệu tiền chiến thuật hoặc kiểm tra lại dữ liệu gốc.")
            hourly_demand_pt = pd.Series(0, index=full_demand_df.index)
            chart_yaxis_title_pt = "Số lượt cất/hạ cánh"
            chart_marker_color_pt = 'gray'
            chart_capacity_value_pt = pd.Series(0, index=full_demand_df.index)
            chart_capacity_name_pt = "Năng lực (Không có dữ liệu)"


        # Lọc dữ liệu theo "Period"
        period_hours_map = {
            "1 Hour": 1, "2 Hours": 2, "3 Hours": 3,
            "6 Hours": 6, "Full Day": 24
        }
        hours_to_display_pt = period_hours_map.get(chart_period_pt, 24)

        chart_start_time = datetime.combine(st.session_state.selected_date, time(0,0,0))
        chart_end_time = datetime.combine(st.session_state.selected_date, time(23,0,0)) + timedelta(minutes=59, seconds=59)

        if chart_period_pt != "Full Day":
            current_hour_slider = st.slider(
                "Chọn giờ bắt đầu cho khung thời gian:",
                min_value=0, max_value=23, value=8, key="pt_start_hour_slider"
            )
            chart_start_time = datetime.combine(st.session_state.selected_date, time(current_hour_slider, 0, 0))
            chart_end_time = chart_start_time + timedelta(hours=hours_to_display_pt) - timedelta(seconds=1)

        filtered_demand_index = hourly_demand_pt.index[
            (hourly_demand_pt.index >= chart_start_time) &
            (hourly_demand_pt.index <= chart_end_time)
        ]

        hourly_demand_pt_filtered = hourly_demand_pt.loc[filtered_demand_index]
        chart_capacity_value_pt_filtered = chart_capacity_value_pt.loc[filtered_demand_index]


        # Vẽ Biểu đồ Pre-Tactical Demand
        fig_pre_tactical_demand = go.Figure()

        fig_pre_tactical_demand.add_trace(go.Bar(
            x=hourly_demand_pt_filtered.index,
            y=hourly_demand_pt_filtered.values,
            name=chart_movement_type_pt + " Demand",
            marker_color=chart_marker_color_pt
        ))

        fig_pre_tactical_demand.add_trace(go.Scatter(
            x=chart_capacity_value_pt_filtered.index,
            y=chart_capacity_value_pt_filtered.values,
            mode='lines',
            name=chart_capacity_name_pt,
            line=dict(color='red', dash='dash', width=3)
        ))

        fig_pre_tactical_demand.update_layout(
            title=f'Pre-tactical Demand Forecast: {chart_forecast_type} - {chart_movement_type_pt} Demand',
            xaxis_title=f'Thời gian (Giờ địa phương - UTC+{VVTS_CONFIG["airport_timezone_offset_hours"]})',
            yaxis_title='Số lượt cất/hạ cánh',
            plot_bgcolor='rgba(0,0,0,0)',
            hovermode="x unified",
            xaxis_tickformat="%H:%M<br>%d/%m",
            xaxis_range=[chart_start_time, chart_end_time]
        )
        st.plotly_chart(fig_pre_tactical_demand, use_container_width=True)

with tab_gdp: # Nội dung Tab 3
    st.header(f"Tactical (GDP Simulation Results) (Ngày {st.session_state.selected_date.strftime('%d/%m/%Y')})")
    # Nút để kích hoạt chạy GDP
    if st.button("Mô phỏng Ground Delay Programme", key="apply_gdp_button_main"):
            if st.session_state.pre_tactical_demand_data.empty:
                st.error("Vui lòng tạo 'Dữ liệu Dự đoán Tiền Chiến thuật' ở Tab 2 trước khi chạy GDP.")
            else:
                with st.spinner("Đang chạy mô phỏng..."):
                    # BƯỚC 1: Chạy GDP để có lịch trình lý tưởng
                    ideal_regulated_data = run_dual_pass_gdp_simulation(
                        st.session_state.pre_tactical_demand_data,
                        st.session_state.takeoff_capacity,
                        st.session_state.landing_capacity,
                        st.session_state.reduced_capacity_events,
                        VVTS_CONFIG['airport_timezone_offset_hours']
                    )
                    
                    # BƯỚC 2: Mô phỏng sự tuân thủ trong thực tế với dung sai
                    # Kết quả cuối cùng có cột 'actual_time_utc' sẽ được lưu lại vào session_state
                    st.session_state.regulated_flights_data = simulate_ctot_compliance(ideal_regulated_data)
                    
                st.session_state.simulation_run = True
                # Rất quan trọng: Chạy lại ứng dụng để tải lại giao diện với dữ liệu mới nhất
                st.rerun()
                # st.rerun() # Thêm lệnh này để tự động cập nhật giao diện sau khi chạy xong

    # --- PHẦN HIỂN THỊ KẾT QUẢ VÀ BIỂU ĐỒ GIỮ NGUYÊN NHƯ PHIÊN BẢN TRƯỚC ---
    if st.session_state.simulation_run and not st.session_state.regulated_flights_data.empty:
        df_regulated_full = st.session_state.regulated_flights_data.copy()
        
        # --- BƯỚC 1: TÍNH TOÁN DỮ LIỆU GOM NHÓM (RESAMPLE) ---
        # Widget chọn độ phân giải thời gian
        agg_period_options = {'1 giờ': 'H', '30 phút': '30T', '15 phút': '15T'}
        selected_agg_label = st.radio(
            "Chọn độ phân giải thời gian hiển thị:",
            options=list(agg_period_options.keys()),
            horizontal=True,
            index=0,
            key="agg_period_selector"
        )
        pandas_freq = agg_period_options[selected_agg_label]
        
        # Tạo một DataFrame rỗng với đầy đủ các mốc thời gian trong ngày
        start_time = datetime.combine(st.session_state.selected_date, time(0, 0))
        end_time = datetime.combine(st.session_state.selected_date, time(23, 59))
        full_time_index = pd.date_range(start=start_time, end=end_time, freq=pandas_freq)
        resampled_df = pd.DataFrame(index=full_time_index)

        # Tách và làm sạch dữ liệu ban đầu và sau điều tiết
        initial_flights_df = df_regulated_full.dropna(subset=['predicted_event_time_local'])
        df_regulated_full['actual_time_local'] = df_regulated_full['actual_time_utc'] + timedelta(hours=VVTS_CONFIG['airport_timezone_offset_hours'])
        regulated_flights_df = df_regulated_full.dropna(subset=['actual_time_local'])

        # Gom nhóm dữ liệu theo giờ
        initial_arr_demand = initial_flights_df[initial_flights_df['flight_type']=='arrival'].set_index('predicted_event_time_local').resample(pandas_freq).size()
        initial_dep_demand = initial_flights_df[initial_flights_df['flight_type']=='departure'].set_index('predicted_event_time_local').resample(pandas_freq).size()
        regulated_arr_demand = regulated_flights_df[regulated_flights_df['flight_type']=='arrival'].set_index('actual_time_local').resample(pandas_freq).size()
        regulated_dep_demand = regulated_flights_df[regulated_flights_df['flight_type']=='departure'].set_index('actual_time_local').resample(pandas_freq).size()

        # Điền dữ liệu đã gom nhóm vào DataFrame chính
        resampled_df['initial_arrival_demand'] = initial_arr_demand.reindex(full_time_index, fill_value=0)
        resampled_df['initial_departure_demand'] = initial_dep_demand.reindex(full_time_index, fill_value=0)
        resampled_df['regulated_arrival_demand'] = regulated_arr_demand.reindex(full_time_index, fill_value=0)
        resampled_df['regulated_departure_demand'] = regulated_dep_demand.reindex(full_time_index, fill_value=0)

        # Tính toán đường năng lực tương ứng với độ phân giải
        if pandas_freq == 'H':
            scaling_factor = 1
        elif pandas_freq == '30T':
            scaling_factor = 2
        else: # 15T
            scaling_factor = 4
        scaled_total_capacity = (st.session_state.landing_capacity + st.session_state.takeoff_capacity) / scaling_factor
        resampled_df['scaled_total_capacity'] = scaled_total_capacity
        # (Bạn có thể thêm code áp dụng sự kiện giảm năng lực ở đây nếu muốn)

        # --- BƯỚC 2: VẼ CÁC BIỂU ĐỒ ---
        st.subheader("So sánh trực quan trước và sau khi áp dụng GDP")

        # Tính toán Y-axis chung để 2 biểu đồ có cùng tỷ lệ (cách an toàn)
        combined_initial_demand = resampled_df['initial_arrival_demand'] + resampled_df['initial_departure_demand']
        if not combined_initial_demand.empty and combined_initial_demand.max() > 0:
            max_y = combined_initial_demand.max() * 1.15
        else:
            max_y = 50 

        # Tạo layout 2 cột
        col1, col2 = st.columns(2)

        # BIỂU ĐỒ 1: TRƯỚC ĐIỀU TIẾT
        with col1:
            st.markdown("Trước khi áp dụng GDP")
            fig_before = go.Figure()
            fig_before.add_trace(go.Bar(x=resampled_df.index, y=resampled_df['initial_departure_demand'], name='Cất cánh', marker_color='#80B4E0'))
            fig_before.add_trace(go.Bar(x=resampled_df.index, y=resampled_df['initial_arrival_demand'], name='Hạ cánh', marker_color='#A0D498'))
            fig_before.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df['scaled_total_capacity'], name='Năng lực', mode='lines', line=dict(color='red', dash='dash', width=2)))
            fig_before.update_layout(barmode='stack', yaxis_title='Số lượt cất/hạ cánh', plot_bgcolor='rgba(240, 240, 240, 0.95)', hovermode="x unified", xaxis_tickformat="%H:%M", margin=dict(l=40, r=20, t=40, b=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), yaxis_range=[0, max_y])
            st.plotly_chart(fig_before, use_container_width=True)

        # BIỂU ĐỒ 2: SAU ĐIỀU TIẾT
        with col2:
            st.markdown("Sau khi áp dụng GDP")
            fig_after = go.Figure()
            fig_after.add_trace(go.Bar(x=resampled_df.index, y=resampled_df['regulated_departure_demand'], name='Cất cánh', marker_color='#005A9E'))
            fig_after.add_trace(go.Bar(x=resampled_df.index, y=resampled_df['regulated_arrival_demand'], name='Hạ cánh', marker_color='#2E8540'))
            fig_after.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df['scaled_total_capacity'], name='Năng lực', mode='lines', line=dict(color='red', dash='dash', width=2)))
            fig_after.update_layout(barmode='stack', plot_bgcolor='rgba(240, 240, 240, 0.95)', hovermode="x unified", xaxis_tickformat="%H:%M", margin=dict(l=40, r=20, t=40, b=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), yaxis_range=[0, max_y])
            st.plotly_chart(fig_after, use_container_width=True)
        # --- BƯỚC 3: HIỂN THỊ BẢNG CHI TIẾT CÁC CHUYẾN BAY BỊ ĐIỀU TIẾT ---
        st.markdown("---")
        st.subheader("Chi tiết thay đổi CTOT của các chuyến bay")

        # Lọc ra các chuyến bay thực sự bị điều tiết (có delay > 0.1 phút)
        regulated_flights_only_df = df_regulated_full[df_regulated_full['is_regulated'] == True].copy()

        # Làm tròn số phút trễ để dễ đọc
        regulated_flights_only_df['atfm_delay_minutes'] = regulated_flights_only_df['atfm_delay_minutes'].round(1)

        # Tách ra 2 loại: Arrival và Departure
        regulated_arrivals_df = regulated_flights_only_df[regulated_flights_only_df['flight_type'] == 'arrival'].copy()
        regulated_departures_df = regulated_flights_only_df[regulated_flights_only_df['flight_type'] == 'departure'].copy()

        # Tạo layout 2 cột cho 2 bảng
        col1_table, col2_table = st.columns(2)

        # BẢNG 1: CHI TIẾT CHUYẾN BAY HẠ CÁNH (ARRIVALS) BỊ ĐIỀU TIẾT
        with col1_table:
            st.markdown("Arrivals")
            if not regulated_arrivals_df.empty:
                # Sắp xếp theo độ trễ giảm dần
                regulated_arrivals_df.sort_values(by='atfm_delay_minutes', ascending=False, inplace=True)
                
                # Các cột cần hiển thị cho Arrival
                display_cols_arr = {
                    'callsign': 'Số hiệu',
                    'origin': 'Sân bay đi',
                    'original_scheduled_time_local': 'ELDT gốc',
                    'regulated_time_local': 'CLDT mới',
                    'ctot_new_local': 'CTOT yêu cầu',
                    'atfm_delay_minutes': 'Phút trễ'
                }

                st.dataframe(
                    regulated_arrivals_df[display_cols_arr.keys()].rename(columns=display_cols_arr),
                    use_container_width=True,
                    hide_index=True
                )
                st.caption("ELDT: Giờ hạ cánh dự kiến; CLDT: Giờ hạ cánh tính toán; CTOT: Giờ cất cánh tính toán.")

            else:
                st.info("Không có chuyến bay hạ cánh nào bị điều tiết.")

        # BẢNG 2: CHI TIẾT CHUYẾN BAY CẤT CÁNH (DEPARTURES) BỊ ĐIỀU TIẾT
        with col2_table:
            st.markdown("Departures")
            if not regulated_departures_df.empty:
                # Sắp xếp theo độ trễ giảm dần
                regulated_departures_df.sort_values(by='atfm_delay_minutes', ascending=False, inplace=True)

                # Các cột cần hiển thị cho Departure
                display_cols_dep = {
                    'callsign': 'Số hiệu',
                    'destination': 'Sân bay đến',
                    'original_scheduled_time_local': 'ETOT gốc',
                    'regulated_time_local': 'CTOT mới',
                    'atfm_delay_minutes': 'Phút trễ'
                }
                
                st.dataframe(
                    regulated_departures_df[display_cols_dep.keys()].rename(columns=display_cols_dep),
                    use_container_width=True,
                    hide_index=True
                )
                st.caption("ETOT: Giờ cất cánh dự kiến; CTOT: Giờ cất cánh tính toán.")

            else:
                st.info("Không có chuyến bay cất cánh nào bị điều tiết.")

        # --- BƯỚC 4: HIỂN THỊ THỐNG KÊ CHUNG ---
        st.markdown("---")
        st.subheader("Thống kê chung")
        
        total_regulated = df_regulated_full['is_regulated'].sum()
        total_delay = df_regulated_full['atfm_delay_minutes'].sum()
        avg_delay = total_delay / total_regulated if total_regulated > 0 else 0
        
        stat_col1, stat_col2, stat_col3 = st.columns(3)
        stat_col1.metric("Số chuyến bay bị điều tiết", f"{total_regulated}")
        stat_col2.metric("Tổng phút trễ ATFM", f"{total_delay:,.0f} phút")
        stat_col3.metric("Độ trễ trung bình", f"{avg_delay:.1f} phút/chuyến")    
        # --- BƯỚC 3: HIỂN THỊ BẢNG CHI TIẾT VÀ THỐNG KÊ ---
        st.markdown("---")
        # (Code hiển thị bảng "Chi tiết các chuyến bay bị điều tiết" và "Thống kê chung" giữ nguyên như cũ)
        # Ví dụ:
        # st.subheader("2.2. Chi tiết các Chuyến bay bị Điều tiết...")
        # ... code bảng chi tiết ...
        # st.subheader("2.3. Thống kê chung...")
        # ... code thống kê ...

    elif st.session_state.simulation_run:
        st.info("Mô phỏng đã chạy nhưng không có dữ liệu kết quả cho ngày đã chọn.")
    else:
        st.info("Thiết lập các thông số ở thanh bên trái và nhấn nút 'Chạy Mô phỏng Điều tiết (GDP)' để xem kết quả.")
