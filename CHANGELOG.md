# Bản tinh gọn — 26/10/2026

- Gộp ba file dependency WSL vào requirements.txt; giữ nguyên phiên bản, loại trùng PyArrow.
- Giữ infra/serving/requirements.txt cho Docker serving; Airflow quản lý dependency trong Dockerfile.
- Bỏ legacy/: patch đã áp dụng, script xuất ZIP trao đổi, README và checksum addon cũ.
- Bỏ evidence/ và validation/ cũ khỏi ZIP triển khai; bằng chứng/dữ liệu gốc của nhóm không bị xóa. Giữ kết luận kiểm thử trong RELEASE_VALIDATION.md.
- Bỏ nhận xét kế hoạch cũ và ba tài liệu phân loại trùng nhau; README là điểm bắt đầu chính.
- Bỏ downloader Network thử nghiệm không được pipeline/tests gọi; giữ manifest kiểm tra đúng 28 CSV.
- Bỏ synthetic_comparison.json không có tham chiếu trong code/tests.
- Giữ hướng dẫn benchmark ở docs/BENCHMARK.md; bỏ đoạn cài addon không còn cần.
- Đưa truy vấn tích hợp về sql/mediation.sql.
- Giữ tests/fixtures, script chẩn đoán và helper patch mà tests còn import; giữ template và DAG được cài.
- Giữ nguyên ba experiment, metadata và scores; identity không thay đổi.
- Giữ nguyên byte mã xử lý còn lại, Dockerfile, compose và mapping/time rules.

Chỉ thay bản ZIP bàn giao. Không sửa project đang chạy, Docker volumes hay dữ liệu trên Drive.
Giải nén vào thư mục mới; chép đè lên bản cũ sẽ không loại được những file đã bỏ.
