from __future__ import annotations
from datetime import datetime, timedelta
from typing import Iterable, List, Optional
from collections import defaultdict
from .models import Flight, CapacityEvent

def _effective_params_at(
    ts: datetime,
    default_aar: int,
    default_adr: int,
    default_sep: int,
    events: Optional[List[CapacityEvent]],
):
    aar, adr, sep = default_aar, default_adr, default_sep
    if events:
        for ev in events:
            if ev.start_time_utc <= ts < ev.end_time_utc:
                aar, adr, sep = ev.aar, ev.adr, ev.min_sep_minutes
                break
    return aar, adr, sep

def allocate_slots(
    flights: Iterable[Flight],
    default_aar: int = 30,
    default_adr: int = 30,
    default_min_sep: int = 2,
    events: Optional[List[CapacityEvent]] = None,
) -> List[Flight]:
    """
    Greedy slot allocator:
      - Phân biệt AAR (arrival) và ADR (departure).
      - Separation kiểm tra riêng theo hàng đợi ARR/DEP (không khoá chéo không cần thiết).
      - Nhảy đầu giờ tiếp theo nếu giờ hiện tại đã đầy theo loại (ARR/DEP).
      - Giới hạn dịch tối đa 12 giờ để tránh vòng lặp bất tận.
    """
    flights_sorted = sorted(list(flights), key=lambda f: (f.eobt_utc, f.callsign))
    allocated_arr: List[datetime] = []
    allocated_dep: List[datetime] = []

    hourly_counts_arr = defaultdict(int)  # {(Y,M,D,H): count}
    hourly_counts_dep = defaultdict(int)

    MAX_SHIFT_MINUTES = 12 * 60

    for f in flights_sorted:
        t = f.eobt_utc.replace(second=0, microsecond=0)
        shifted = 0
        while shifted <= MAX_SHIFT_MINUTES:
            aar, adr, sep = _effective_params_at(t, default_aar, default_adr, default_min_sep, events)
            hour_key = (t.year, t.month, t.day, t.hour)

            if f.is_arrival:
                hour_demand = hourly_counts_arr[hour_key]
                cap = aar
                seq = a
