
# Refactor: Tách lớp dữ liệu/thuật toán/giao diện cho ATFM

## Nội dung
- Thêm mô-đun `src/`:
  - `src/models.py`: `Flight`, `Schedule`, `CapacityEvent`
  - `src/allocator.py`: thuật toán phân bổ slot (AAR/ADR, separation, sự kiện giảm năng lực)
  - `src/utils.py`: parse CSV lịch bay và tạo dict flight
- Thêm `app_refactored.py`: Streamlit app mới, cấu hình AAR/ADR, separation, và 1 sự kiện giảm năng lực mẫu.

## Cách dùng
1. Cài thư viện: `pip install -r requirements.txt` (cần có streamlit, pandas, numpy, plotly).
2. Chạy: `streamlit run app_refactored.py`

## Tích hợp vào repo
- Copy thư mục `src/` và file `app_refactored.py` vào root của repo.
- Commit với message: `refactor: modularize ATFM and add new Streamlit app`

## Ghi chú
- Thuật toán phân bổ hiện là Greedy + kiểm tra theo phút; đã tách AAR/ADR, có sự kiện giảm năng lực. Có thể mở rộng GDP/slot swapping sau.
