
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class Flight:
    callsign: str
    origin: str
    destination: str
    eobt_utc: datetime  # Estimated Off-Block Time in UTC
    aircraft: str
    is_arrival: bool
    slot_time_utc: Optional[datetime] = None
    delay_minutes: float = 0.0
    regulated: bool = False

@dataclass
class Schedule:
    flights: List[Flight] = field(default_factory=list)

@dataclass
class CapacityEvent:
    start_time_utc: datetime
    end_time_utc: datetime
    aar: int  # arrival capacity per hour
    adr: int  # departure capacity per hour
    min_sep_minutes: int = 2  # optional tighter/looser separation during the event
