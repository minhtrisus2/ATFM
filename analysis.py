# atfm_core/analysis.py

import pandas as pd
from datetime import datetime, time

def analyze_hourly_demand(flights_df, time_column_local, landing_capacity, takeoff_capacity):
    """
    Hàm tổng quát để phân tích nhu cầu theo giờ từ một cột thời gian cụ thể.
    """
    if flights_df is None or flights_df.empty or time_column_local not in flights_df.columns:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    analysis_date = pd.to_datetime(flights_df[time_column_local].iloc[0]).date()
    # Tạo index với múi giờ địa phương để đảm bảo tính nhất quán
    hourly_index = pd.date_range(start=datetime.combine(analysis_date, time.min), periods=24, freq='H', tz='Asia/Ho_Chi_Minh')
    
    analysis_df = pd.DataFrame(index=hourly_index)
    
    # Chuyển đổi cột thời gian sang timezone-aware để groupby một cách an toàn
    flights_df_aware = flights_df.copy()
    flights_df_aware[time_column_local] = pd.to_datetime(flights_df_aware[time_column_local]).dt.tz_localize('Asia/Ho_Chi_Minh', ambiguous='NaT', nonexistent='raise')
    
    analysis_df['arrival_demand'] = flights_df_aware[flights_df_aware['flight_type']=='arrival'].groupby(flights_df_aware[time_column_local].dt.floor('H')).size().reindex(analysis_df.index, fill_value=0)
    analysis_df['departure_demand'] = flights_df_aware[flights_df_aware['flight_type']=='departure'].groupby(flights_df_aware[time_column_local].dt.floor('H')).size().reindex(analysis_df.index, fill_value=0)
    analysis_df['landing_capacity'] = landing_capacity
    analysis_df['takeoff_capacity'] = takeoff_capacity

    # Xác định các điểm nóng
    arrival_hotspots = analysis_df[analysis_df['arrival_demand'] > analysis_df['landing_capacity']]
    departure_hotspots = analysis_df[analysis_df['departure_demand'] > analysis_df['takeoff_capacity']]
    
    return analysis_df, arrival_hotspots, departure_hotspots

def generate_advisory_messages(arrival_hotspots, departure_hotspots):
    # (Hàm này giữ nguyên như cũ)
    advisories = []
    if not arrival_hotspots.empty: advisories.append({"type": "warning", "title": "CẢNH BÁO: QUÁ TẢI LUỒNG HẠ CÁNH", "body": f"Phát hiện **{len(arrival_hotspots)}** khung giờ có nhu cầu hạ cánh vượt năng lực. Khuyến nghị áp dụng GDP."})
    if not departure_hotspots.empty: advisories.append({"type": "warning", "title": "CẢNH BÁO: QUÁ TẢI LUỒNG CẤT CÁNH", "body": f"Phát hiện **{len(departure_hotspots)}** khung giờ có nhu cầu cất cánh vượt năng lực. Khuyến nghị áp dụng các biện pháp điều tiết."})
    if not advisories: advisories.append({"type": "success", "title": "TRẠNG THÁI: BÌNH THƯỜNG", "body": "Nhu cầu dự đoán trong ngày nằm trong giới hạn năng lực."})
    return advisories