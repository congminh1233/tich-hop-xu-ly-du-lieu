# Kiểm tra bản tinh gọn — 26/10/2026

## Kiểm tra lần này

- Chạy lại 5 tests công cụ đóng gói/kiểm tra CSV: tất cả passed.
- Parse cú pháp mọi file Python còn trong gói.
- Requirements mới bằng hợp của ba file cũ: đủ dependency, không đổi phiên bản, không khai báo trùng.
- SHA256 các file Python giữ lại khớp bản release trước.
- Hash ETL vẫn bằng 45c60d9ceb4e098e6253ee479b5fa701fc12d700c12dfa7b02b0b1131bae6ab3.
- Kiểm tra liên kết tài liệu tương đối, manifest, CRC ZIP và payload sau giải nén.

## Kết quả lịch sử của bản trước

Bản trước đã chạy 83 tests passed, 1 PostgreSQL test skipped, 2 tests parse/render DAG
Airflow deselected do thiếu runtime/service. Spark local dùng dữ liệu tổng hợp;
dashboard dùng AppTest. Ba bộ scores có tổng cộng 155.623 dòng đã được kiểm tra
qua adapter serving. Đây không phải số test chạy lại trong lần tinh gọn.
Mã xử lý và các experiment được giữ nguyên byte.

## Giới hạn

Chỉ thay tổ chức file, dependency và tài liệu; không chạy lại full ETL hoặc benchmark
27 lượt. Chưa dựng Docker/HDFS/Airflow/PostgreSQL thật ở đây, chưa xác nhận fresh
install trên máy thành viên. Cần chạy README trên máy đích để xác nhận; kiểm tra
ZIP không thay thế kiểm thử toàn hệ thống.
