# atfm_core/system_state.py

import pandas as pd
from datetime import datetime, time, date

class SystemState:
    """
    Lớp quản lý toàn bộ trạng thái của hệ thống mô phỏng.
    Hoạt động như một "single source of truth" mô phỏng kho dữ liệu trên cloud.
    """
    def __init__(self, master_schedule_df):
        self.master_schedule = master_schedule_df.copy()
        # Khởi tạo thời gian mô phỏng là thời điểm EOBT đầu tiên trong ngày
        self.simulation_time = self.master_schedule['eobt_local'].min()
        self.is_gdp_active = False
        self.regulated_schedule = None

    def get_flights_by_status(self, current_time):
        """
        Phân loại các chuyến bay dựa trên thời gian mô phỏng hiện tại.
        """
        # Chưa đến giờ cất cánh
        future_flights = self.master_schedule[self.master_schedule['eobt_local'] > current_time]
        
        # Đang hoạt động (đã qua EOBT nhưng chưa qua giờ hạ cánh/cất cánh tại VVTS)
        active_flights = self.master_schedule[
            (self.master_schedule['eobt_local'] <= current_time) &
            (self.master_schedule['event_time_local'] > current_time)
        ]
        
        # Đã hoàn thành
        completed_flights = self.master_schedule[self.master_schedule['event_time_local'] <= current_time]
        
        return future_flights, active_flights, completed_flights

    def update_simulation_time(self, new_time):
        self.simulation_time = new_time

    def activate_gdp(self, regulated_df):
        """Cập nhật trạng thái hệ thống khi GDP được kích hoạt."""
        self.is_gdp_active = True
        self.regulated_schedule = regulated_df
        
        # Cập nhật lại lịch trình chính với các thông số điều tiết
        # Dùng một bản sao để tránh lỗi SettingWithCopyWarning
        temp_df = self.master_schedule.copy()
        
        # Tạo key duy nhất để merge
        temp_df['merge_key'] = temp_df['callsign'] + temp_df['flight_date'].astype(str)
        regulated_df['merge_key'] = regulated_df['callsign'] + regulated_df['flight_date'].astype(str)
        
        # Merge các thông tin điều tiết
        update_cols = ['merge_key', 'regulated_time_utc', 'is_regulated', 'atfm_delay_minutes']
        temp_df = temp_df.merge(
            regulated_df[update_cols],
            on='merge_key',
            how='left',
            suffixes=('', '_new')
        )
        
        # Cập nhật lại thời gian sự kiện nếu bị điều tiết
        regulated_mask = temp_df['is_regulated_new'].fillna(False)
        temp_df.loc[regulated_mask, 'event_time_utc'] = temp_df.loc[regulated_mask, 'regulated_time_utc_new']
        
        # Cập nhật các cột chính
        self.master_schedule['event_time_utc'] = temp_df['event_time_utc']
        self.master_schedule['is_regulated'] = temp_df['is_regulated_new'].fillna(False)