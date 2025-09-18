import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List
from src.models import Flight, Schedule, CapacityEvent
from src.allocator import allocate_slots
from src.utils import parse_schedule_df, flight_dicts_from_df

st.set_page_config(page_title="ATFM Slot Allocation Simulator", layout="wide")
st.title("ATFM Slot Allocation Simulator (Refactored)")

st.markdown("Tải file **vvts_schedule.csv** để mô phỏng. Định dạng cột giống file mẫu trong repo.")

with st.sidebar:
    st.header("Cấu hình mô phỏng")
    default_aar = st.number_input("AAR (arrival/h)", min_value=1, max_value=120, value=30, step=1)
    default_adr = st.number_input("ADR (departure/h)", min_value=1, max_value=120, value=30, step=1)
    default_sep = st.number_input("Separation tối thiểu (phút)", min_value=1, max_value=10, value=2, step=1)
    tz_offset = st.number_input("UTC offset của VVTS (giờ)", min_value=-12, max_value=14, value=7, step=1)

    st.subheader("Sự kiện giảm năng lực (tùy chọn)")
    use_event = st.checkbox("Bật một sự kiện giảm năng lực mẫu", value=False)
    events = None
    if use_event:
        col1, col2 = st.columns(2)
        with col1:
            start_dt = st.text_input("Bắt đầu (YYYY-MM-DD HH:MM)", value="2025-06-23 06:00")
        with col2:
            end_dt = st.text_input("Kết thúc (YYYY-MM-DD HH:MM)", value="2025-06-23 10:00")
        aar_ev = st.number_input("AAR (trong sự kiện)", min_value=1, max_value=120, value=20, step=1)
        adr_ev = st.number_input("ADR (trong sự kiện)", min_value=1, max_value=120, value=20, step=1)
        sep_ev = st.number_input("Separation (trong sự kiện, phút)", min_value=1, max_value=10, value=3, step=1)

        try:
            ev = CapacityEvent(
                start_time_utc=datetime.strptime(start_dt, "%Y-%m-%d %H:%M"),
                end_time_utc=datetime.strptime(end_dt, "%Y-%m-%d %H:%M"),
                aar=aar_ev, adr=adr_ev, min_sep_minutes=sep_ev
            )
            events = [ev]
        except Exception as e:
            st.warning(f"Không thể parse thời gian sự kiện: {e}")

uploaded = st.file_uploader("Chọn file vvts_schedule.csv", type=["csv"])

if uploaded is not None:
    df = pd.read_csv(uploaded)
    try:
        df = parse_schedule_df(df)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Build Flight objects (arrival dùng ETA nếu có trong eets.csv)
    flights: List[Flight] = []
    used_eet_count = 0
    total_arrivals = 0
    for d in flight_dicts_from_df(df, eets_csv_path="eets.csv"):
        flights.append(Flight(
            callsign=d["callsign"],
            origin=d["origin"],
            destination=d["destination"],
            eobt_utc=d["eobt_utc"],
            aircraft=d["aircraft"],
            is_arrival=d["is_arrival"]
        ))
        if d["is_arrival"]:
            total_arrivals += 1
            if d.get("used_eet"):
                used_eet_count += 1

    if total_arrivals > 0 and used_eet_count < total_arrivals:
        st.warning(f"{total_arrivals - used_eet_count} chuyến ARRIVAL không tìm thấy EET trong eets.csv → ETA dùng EOBT mặc định.")

    # Phân bổ slot
    allocated = allocate_slots(
        flights,
        default_aar=default_aar,
        default_adr=default_adr,
        default_min_sep=default_sep,
        events=events,
    )

    # Kết quả
    res = pd.DataFrame([{
        "callsign": f.callsign,
        "origin": f.origin,
        "destination": f.destination,
        "EOBT/ETA_UTC": f.eobt_utc,
        "Slot_UTC": f.slot_time_utc,
        "Slot_Local": (pd.to_datetime(f.slot_time_utc) + pd.to_timedelta(tz_offset, unit="h")) if f.slot_time_utc else None,
        "Delay_min": round(f.delay_minutes, 1),
        "Regulated": f.regulated
    } for f in allocated]).sort_values(["Slot_UTC", "callsign"])

    st.subheader("Kết quả phân bổ")
    st.dataframe(res, use_container_width=True)

    # Biểu đồ đơn giản: phân phối delay
    st.subheader("Phân phối delay (phút)")
    st.bar_chart(res["Delay_min"])

    # Biểu đồ demand vs capacity theo giờ (UTC)
    st.subheader("Demand vs Capacity theo giờ (UTC)")
    tmp = res.copy()
    tmp["hour"] = pd.to_datetime(tmp["Slot_UTC"]).dt.floor("h")
    demand_hour = tmp.groupby("hour").size().rename("demand_total").reset_index()
    # capacity "tổng" = AAR + ADR (xấp xỉ; mô hình chi tiết runway sẽ tách riêng)
    demand_hour["capacity_total"] = default_aar + default_adr
    st.line_chart(demand_hour.set_index("hour"))

    st.download_button(
        label="Tải kết quả CSV",
        data=res.to_csv(index=False).encode("utf-8"),
        file_name="slot_allocation_result.csv",
        mime="text/csv"
    )
else:
    st.info("Hãy tải lên vvts_schedule.csv để chạy mô phỏng.")
