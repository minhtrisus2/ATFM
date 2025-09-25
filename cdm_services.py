# atfm_core/cdm_services.py

def validate_slot_swap(flight1, flight2):
    """
    Kiểm tra xem việc hoán đổi slot giữa hai chuyến bay có hợp lệ không.
    
    Returns:
        (bool, str): (True/False, "Lý do nếu không hợp lệ")
    """
    # 1. Kiểm tra cơ bản
    if flight1['flight_type'] != flight2['flight_type']:
        return False, "Không thể hoán đổi slot giữa chuyến bay đến và chuyến bay đi."
    if flight1['callsign'][:3] != flight2['callsign'][:3]:
        return False, "Chỉ có thể hoán đổi slot giữa các chuyến bay của cùng một hãng."

    # 2. Lấy thời gian mới sau khi hoán đổi
    new_time1 = flight2['regulated_time_utc']
    new_time2 = flight1['regulated_time_utc']

    # 3. Kiểm tra xem thời gian mới có sớm hơn thời gian gốc không (không cho phép bay sớm hơn)
    if new_time1 < flight1['event_time_utc'] or new_time2 < flight2['event_time_utc']:
        return False, "Không thể hoán đổi để bay sớm hơn thời gian dự kiến ban đầu."

    # (Các logic kiểm tra phức tạp hơn về năng lực có thể được thêm vào đây trong tương lai)

    return True, "Việc hoán đổi slot hợp lệ."