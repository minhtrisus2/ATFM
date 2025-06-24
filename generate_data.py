import pandas as pd
from datetime import date, timedelta
import random
import string 

# --- Cấu hình Tạo Dữ liệu ---
NUM_FLIGHTS = 1002 # Tăng số lượng chuyến bay để tạo nhiều tình huống tắc nghẽn
NUM_DAYS = 7      # Mô phỏng trong 7 ngày liên tiếp

# Sân bay Tân Sơn Nhất
HOME_AIRPORT = 'VVTS'

# Danh sách các sân bay (Mã ICAO: Tên thành phố)
airports = {
    'VVDN': 'Đà Nẵng', 'VVPB': 'Phú Quốc', 'VVKR': 'Cam Ranh (Nha Trang)',
    'VVCT': 'Cần Thơ', 'VVCS': 'Côn Đảo', 'VVDL': 'Đà Lạt',
    'VVGL': 'Pleiku', 'VVNB': 'Nội Bài (Hà Nội)', 'VVBM': 'Buôn Ma Thuột',
    'VVTH': 'Thọ Xuân (Thanh Hóa)', 'VVCI': 'Chu Lai', 'VVPC': 'Tuy Hòa',
    'VVPK': 'Phú Cát (Quy Nhơn)', 'VVCA': 'Cà Mau',
    'VVTS': 'Tân Sơn Nhất (TP.HCM)', 

    'WSSS': 'Singapore (Changi)', 'VTBS': 'Bangkok (Suvarnabhumi)',
    'WMKK': 'Kuala Lumpur (KLIA)', 'RPLL': 'Manila (Ninoy Aquino)',
    'VHHH': 'Hong Kong (Chek Lap Kok)', 'RCTP': 'Taipei (Taoyuan)',
    'RKSI': 'Seoul (Incheon)', 'RJAA': 'Tokyo (Narita)',
    'ZGGG': 'Guangzhou (Baiyun)', 'ZSPD': 'Shanghai (Pudong)',
    'VIDP': 'Delhi (Indira Gandhi)', 'OMDB': 'Dubai (Intl.)',
    'VMMC': 'Ma Cao', 'VVPF': 'Phnom Penh', 'VLPL': 'Vientiane',
    'EGLL': 'London (Heathrow)', 'KJFK': 'New York (JFK)' 
}

# Các hãng hàng không (mã ICAO) và Operator Designator (Callsign ICAO)
airline_callsign_map = {
    'VN': 'HVN', # Vietnam Airlines
    'VJ': 'VJC', # Vietjet Air
    'QH': 'BAV', # Bamboo Airways
    'BL': 'PIC', # Pacific Airlines
    'SQ': 'SIA', # Singapore Airlines
    'TG': 'THA', # Thai Airways
    'AK': 'AXM', # AirAsia (Malaysia) - AXM is operator designator for AirAsia Bhd
    'PR': 'PAL', # Philippine Airlines
    'CX': 'CPA', # Cathay Pacific
    'CI': 'CAL', # China Airlines
    'KE': 'KAL', # Korean Air
    'NH': 'ANA', # All Nippon Airways
    'CZ': 'CSN', # China Southern Airlines
    'MU': 'CES', # China Eastern Airlines
    'AI': 'AIC', # Air India
    'EK': 'UAE', # Emirates
    'FD': 'THD', # Thai AirAsia
    'TR': 'SCO', # Scoot
    'BA': 'BAW', # British Airways
    'DL': 'DAL'  # Delta Air Lines
}
airlines_icao_codes = list(airline_callsign_map.keys())

aircraft_types = ['A320', 'A321', 'B737', 'B787', 'A350', 'B777', 'A330', 'A380'] 

# --- Định nghĩa thời gian bay ước tính (EET), lăn bánh vào (Taxi-in), và lăn bánh ra (Taxi-out) CƠ SỞ và BIẾN ĐỘNG ---
# Key: Airport ICAO Code
# Value: (Base EET to/from VVTS, EET Deviation, Base Taxi-in Time, Taxi-in Deviation, Base Taxi-out Time, Taxi-out Deviation)
airport_times_map = {
    'VVDN': (75, 5, 15, 5, 15, 5),  'VVPB': (60, 5, 10, 5, 10, 5),  'VVKR': (70, 5, 15, 5, 15, 5),
    'VVCT': (45, 5, 10, 5, 10, 5),  'VVCS': (40, 5, 10, 5, 10, 5),  'VVDL': (55, 5, 15, 5, 15, 5),
    'VVGL': (70, 5, 15, 5, 15, 5),  'VVNB': (110, 10, 20, 5, 20, 5), 'VVBM': (70, 5, 15, 5, 15, 5),
    'VVTH': (100, 10, 15, 5, 15, 5), 'VVCI': (85, 5, 15, 5, 15, 5),  'VVPC': (80, 5, 15, 5, 15, 5),
    'VVPK': (75, 5, 15, 5, 15, 5),  'VVCA': (50, 5, 10, 5, 10, 5),
    'VVTS': (0, 0, 15, 5, 15, 5), 

    'WSSS': (120, 10, 20, 5, 20, 5), 'VTBS': (90, 10, 20, 5, 20, 5), 'WMKK': (115, 10, 20, 5, 20, 5),
    'RPLL': (160, 15, 20, 5, 20, 5), 'VVPF': (70, 10, 15, 5, 15, 5), 'VLPL': (100, 10, 15, 5, 15, 5), 

    'VHHH': (160, 15, 25, 5, 25, 5), 'RCTP': (200, 20, 25, 5, 25, 5), 'ZGGG': (180, 15, 25, 5, 25, 5),
    'ZSPD': (240, 20, 25, 5, 25, 5), 'VMMC': (60, 10, 15, 5, 15, 5), 

    'VIDP': (340, 30, 30, 5, 30, 5), 'OMDB': (420, 40, 40, 5, 40, 5),  

    'EGLL': (780, 60, 45, 5, 45, 5), 'KJFK': (900, 60, 45, 5, 45, 5) 
}

# --- Tạo file eets.csv (Airport Metadata) ---
eets_data_records = []
for code, (base_eet, dev_eet, base_taxi_in, dev_taxi_in, base_taxi_out, dev_taxi_out) in airport_times_map.items():
    eet_to_vvts = max(1, base_eet + random.randint(-dev_eet, dev_eet)) 
    eet_from_vvts = max(1, base_eet + random.randint(-dev_eet, dev_eet)) 
    taxi_in_minutes = max(5, base_taxi_in + random.randint(-dev_taxi_in, dev_taxi_in))
    taxi_out_minutes = max(5, base_taxi_out + random.randint(-dev_taxi_out, dev_taxi_out))

    eets_data_records.append({
        'airport_code': code,
        'eet_to_vvts_minutes': eet_to_vvts,
        'eet_from_vvts_minutes': eet_from_vvts,
        'taxi_in_minutes': taxi_in_minutes,
        'taxi_out_minutes': taxi_out_minutes 
    })

eets_df_gen = pd.DataFrame(eets_data_records)
eets_df_gen.to_csv('eets.csv', index=False)
print("Đã tạo file eets.csv thành công với thông tin EET, Taxi-in, Taxi-out!")


# --- Tạo dữ liệu vvts_schedule.csv ---
flight_records = []
current_date = date(2025, 6, 23) 

generated_callsigns_set = set() # Để đảm bảo callsign duy nhất

def generate_unique_icao_callsign(airline_code_icao):
    """Tạo callsign theo format ICAO Operator Designator + Flight Number (duy nhất)"""
    while True:
        flight_number = random.randint(100, 9999) # 3-4 chữ số
        callsign = f"{airline_callsign_map[airline_code_icao]}{flight_number}"
        if callsign not in generated_callsigns_set:
            generated_callsigns_set.add(callsign)
            return callsign

for day_offset in range(NUM_DAYS):
    current_generating_date = current_date + timedelta(days=day_offset)
    
    for hour in range(24):
        if 6 <= hour <= 9 or 11 <= hour <= 14 or 17 <= hour <= 20:
            flights_this_hour_base = 50 
            flights_this_hour_deviation = 25 
        else:
            flights_this_hour_base = 20 
            flights_this_hour_deviation = 10 
        
        flights_this_hour = random.randint(max(5, flights_this_hour_base - flights_this_hour_deviation), 
                                          flights_this_hour_base + flights_this_hour_deviation)
        flights_this_hour = max(5, flights_this_hour) 

        for i in range(flights_this_hour):
            flight_direction = random.choice(['arrival', 'departure'])
            
            all_other_airports_codes = [code for code in airports.keys() if code != HOME_AIRPORT]
            
            domestic_airports = [code for code in all_other_airports_codes if code.startswith('VV')]
            international_airports = [code for code in all_other_airports_codes if not code.startswith('VV')]

            selected_other_airport = None
            if random.random() < 0.50 and domestic_airports: 
                selected_other_airport = random.choice(domestic_airports)
            elif international_airports: 
                selected_other_airport = random.choice(international_airports)
            else: 
                selected_other_airport = random.choice(all_other_airports_codes)


            if flight_direction == 'arrival':
                origin = selected_other_airport
                destination = HOME_AIRPORT
            else: 
                origin = HOME_AIRPORT
                destination = selected_other_airport
            
            minute = random.randint(0, 59)
            eobt = f"{hour:02d}:{minute:02d}"

            # Tạo callsign theo ICAO Operator Designator + Flight Number
            # Đảm bảo chọn một mã hãng hàng không có trong map
            airline_icao_code = random.choice(airlines_icao_codes) 
            callsign = generate_unique_icao_callsign(airline_icao_code)
            
            aircraft = random.choice(aircraft_types)

            flight_records.append({
                'callsign': callsign,
                'origin': origin,
                'destination': destination,
                'eobt': eobt,
                'flight_date': current_generating_date.strftime('%Y-%m-%d'),
                'aircraft_type': aircraft
            })

flights_df_gen_schedule = pd.DataFrame(flight_records)

# Lưu DataFrame ra file CSV
flights_df_gen_schedule.to_csv('vvts_schedule.csv', index=False)

print(f"Đã tạo file vvts_schedule.csv thành công với {len(flights_df_gen_schedule)} chuyến bay trong {NUM_DAYS} ngày!")
print("Nhớ đặt hai file này cùng thư mục với ứng dụng Streamlit của bạn.")
