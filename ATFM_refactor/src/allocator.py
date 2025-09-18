
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Iterable, List, Optional
from .models import Flight, CapacityEvent

def _effective_params_at(ts: datetime, default_aar: int, default_adr: int,
                         default_sep: int, events: Optional[List[CapacityEvent]]):
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
    """Greedy slot allocator with hourly capacity and minute-level separation.
    - Distinguishes AAR vs ADR.
    - Honors reduced capacity events.
    - Avoids minute-by-minute infinite loops with a reasonable cap.
    """
    flights_sorted = sorted(list(flights), key=lambda f: (f.eobt_utc, f.callsign))
    allocated_times: List[datetime] = []

    # Track per-hour demand already assigned
    from collections import defaultdict
    hourly_counts_arr = defaultdict(int)
    hourly_counts_dep = defaultdict(int)

    MAX_SHIFT_MINUTES = 12 * 60  # don't shift more than 12h

    for f in flights_sorted:
        t = f.eobt_utc.replace(second=0, microsecond=0)
        shifted = 0
        while shifted <= MAX_SHIFT_MINUTES:
            aar, adr, sep = _effective_params_at(t, default_aar, default_adr, default_min_sep, events)
            hour_key = (t.year, t.month, t.day, t.hour)

            # choose relevant capacity counter
            if f.is_arrival:
                hour_demand = hourly_counts_arr[hour_key]
                cap = aar
            else:
                hour_demand = hourly_counts_dep[hour_key]
                cap = adr

            if hour_demand < cap:
                # check separation constraint
                ok_sep = True
                for s in allocated_times:
                    if abs((t - s).total_seconds())/60 < sep:
                        ok_sep = False
                        break
                if ok_sep:
                    # assign
                    f.slot_time_utc = t
                    f.delay_minutes = max(0.0, (t - f.eobt_utc).total_seconds()/60.0)
                    f.regulated = f.delay_minutes > 0.1
                    allocated_times.append(t)
                    allocated_times.sort()
                    if f.is_arrival:
                        hourly_counts_arr[hour_key] += 1
                    else:
                        hourly_counts_dep[hour_key] += 1
                    break

            # try next minute or jump hour if hour full
            if hour_demand >= cap:
                t = (t.replace(minute=0) + timedelta(hours=1))
            else:
                t = t + timedelta(minutes=1)
            shifted += 1

        # if not allocated within cap, keep original time to avoid None
        if f.slot_time_utc is None:
            f.slot_time_utc = f.eobt_utc

    return flights_sorted
