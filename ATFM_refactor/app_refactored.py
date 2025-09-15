
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List
from src.models import Flight, Schedule, CapacityEvent
from src.allocator import allocate_slots
from src.utils import parse_schedule_df, flight_dicts_from_df

st.set_page_config(page_title="ATFM Slot Allocation Simulator", layout="wide")
st.title("ATFM Slot Allocation Simulator (Refactored)")

st.markdown("Tải file **vvts_schedule.csv** (định dạng giống repo) để mô phỏng.")

with st.sidebar:
    st.header("Cấu hình mô phỏng")
    default_aar = st.number_input("AAR (arrival/h)", min_value=1, max_value=120, value=30, step=1)
    default_adr = st.number_input("ADR (departure/h)", min_value=1, max_value=120, value=30, step=1)
    default_sep = st.number_input("Separation tối thiểu (phút)", min_value=1, max_value=10, value=2, step=1)

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

    # Build Flight objects
    flights: List[Flight] = []
    for d in flight_dicts_from_df(df):
        flights.append(Flight(**d))

    # Allocate
    allocated = allocate_slots(
        flights,
        default_aar=default_aar,
        default_adr=default_adr,
        default_min_sep=default_sep,
        events=events,
    )

    # Prepare results
    res = pd.DataFrame([{
        "callsign": f.callsign,
        "origin": f.origin,
        "destination": f.destination,
        "EOBT_UTC": f.eobt_utc,
        "Slot_UTC": f.slot_time_utc,
        "Delay_min": round(f.delay_minutes, 1),
        "Regulated": f.regulated
    } for f in allocated])

    st.subheader("Kết quả phân bổ")
    st.dataframe(res, use_container_width=True)

    st.subheader("Phân phối delay (phút)")
    st.bar_chart(res["Delay_min"])

    st.download_button(
        label="Tải kết quả CSV",
        data=res.to_csv(index=False).encode("utf-8"),
        file_name="slot_allocation_result.csv",
        mime="text/csv"
    )
else:
    st.info("Hãy tải lên vvts_schedule.csv để chạy mô phỏng.")
