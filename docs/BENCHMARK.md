# IT5427 — Benchmark Spark Silver → Gold

## Mục đích và phạm vi

Gói đo lại bước đọc Silver Parquet trên HDFS, tổng hợp cửa sổ 60 giây,
liên kết host Network–Windows, tổng hợp IoT và ghi ba bảng Gold mới.
Các hàm ánh xạ host/tổng hợp Network/Windows được import từ mã project đã duyệt.
Không sửa các file ETL/Airflow hiện tại hoặc kết quả thí nghiệm.

Đây **không phải benchmark ETL từ CSV**, không đo Bronze → Silver, huấn luyện
mô hình, PostgreSQL hoặc overhead Airflow. `local[4]` là bốn luồng xử lý trên
một máy; không phải cụm bốn worker/nút.

Nguồn lấy tự động từ full plan đã duyệt trong `infra/airflow/config/runtime.json`.
Với project hiện tại, snapshot nguồn HDFS là `1d32fcc1579230a051c51837`.
Các đầu ra benchmark nằm riêng dưới:

```text
hdfs://namenode:9000/it5427/benchmarks_v1/bench-<profile>-<time>-<id>/
```

Không có nhánh tái sử dụng đầu ra khi đo. Nếu đường dẫn một trial đã tồn tại,
chương trình báo lỗi thay vì lấy thời gian từ snapshot cũ.

## 1. Điều kiện

Các file `infra/benchmark/*.py` đã có trong release. Hoàn thành full HDFS ETL và Airflow configure theo README trước khi benchmark. Không cần cài addon riêng.

## 2. Chuẩn bị tài nguyên

Chỉ làm sau khi các DAG hiện tại đã hoàn tất. Giữ Docker Desktop hoạt động,
giữ máy thức và cắm nguồn trong khi chạy benchmark. Không chạy tác vụ nặng khác.

Tạm dừng hai stack Airflow và serving để hạn chế cạnh tranh CPU/RAM; dữ liệu
volume và file project vẫn được giữ:

```bash
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml stop
docker compose --env-file infra/serving/.env -f infra/serving/compose.yaml stop
```

HDFS cần tiếp tục chạy:

```bash
docker compose -f infra/hdfs/compose.yaml up -d --wait --wait-timeout 300 namenode datanode
python3 infra/benchmark/manage.py check
```

`check` kiểm tra code/plan đã duyệt, image Docker, sức khỏe HDFS, tài nguyên
Docker và các container Airflow/serving còn chạy. Không in mật khẩu.
Trong giai đoạn chuẩn bị Spark kiểm tra thêm dung lượng HDFS.

Cấu hình cố định: driver 4 GiB, container tối đa 6 GiB, shuffle partitions 8,
AQE tắt, file read partition tối đa 128 MiB. Ma trận giữ giới hạn container
4 CPU cho mọi trial, chỉ đổi số luồng `local[1/2/4]`; nếu Docker có dưới 4 CPU,
chương trình dừng trước khi chạy ma trận. Smoke dùng giới hạn 2 CPU.

## 3. Chạy thử nhỏ trước

```bash
python3 infra/benchmark/manage.py smoke
```

Smoke chuẩn bị mẫu khoảng 1% từng bảng Silver bằng hash `event_id`, tạo kết quả
tham chiếu, rồi thực hiện một trial `local[2]` và đối chiếu ba bảng Gold.
Khâu tạo mẫu vẫn phải đọc dữ liệu Silver đầy đủ, nên thời gian smoke không bằng
1% thời gian chạy full. Không dùng smoke để kết luận speedup.

Lệnh chạy đồng bộ: terminal chỉ trả về khi xong hoặc có lỗi. Cứ khoảng 30 giây
in `RUNNING ...`; đây là thông báo tiến trình còn hoạt động, không khẳng định
task Spark đã tiến thêm. Muốn xem log chi tiết, mở terminal thứ hai và dùng
`tail -f` với đường dẫn `.log` được in sau `START`.

Thành công sẽ có:

```text
PREPARE_PASSED
TRIAL_PASSED p01-c2-r1 ...
SUMMARY: passed 1 / 1
BENCHMARK_PASSED smoke
SEND_THIS_FILE: .../reports/hdfs/bench-smoke-....zip
```

Gửi ZIP smoke để kiểm tra trước khi chạy ma trận nếu bạn muốn đi từng bước.
Nếu có lỗi, gói cố gắng vẫn xuất ZIP chẩn đoán. Không trigger DAG Airflow để
chạy benchmark này; launcher chạy trực tiếp container Spark riêng.

## 4. Chạy ma trận benchmark

Sau khi smoke đạt:

```bash
python3 infra/benchmark/manage.py matrix
```

| Tỷ lệ lấy mẫu danh nghĩa | Số luồng Spark | Số lần đo mỗi cấu hình |
|---|---|---:|
| 25% | 1, 2, 4 | 3 |
| 50% | 1, 2, 4 | 3 |
| 100% | 1, 2, 4 | 3 |

Có 27 trial đo. Các tổ hợp được xáo trộn bằng seed cố định trong mỗi vòng lặp.
Tất cả chạy tuần tự. Hãy dành thời gian đủ dài; chưa có số đo trên máy bạn để
dự báo chính xác tổng thời gian. Mỗi container có timeout hai giờ.

**Quy trình thực hiện:**

1. Xác thực metadata và số dòng Silver của snapshot nguồn.
2. Tạo ba tập đầu vào riêng, cùng cách chia 8 partition. Các tập 25/50% là tập
   con lồng nhau theo `pmod(xxhash64(event_id),100)`; mọi metric của cùng event
   được chọn cùng nhau. Tỷ lệ thực tế có thể khác nhẹ tỷ lệ danh nghĩa.
3. Tạo Gold tham chiếu cho từng tập. Ở mức 100%, đối chiếu với Gold chính thức
   để kiểm tra việc triển khai workload benchmark.
4. Mỗi trial mở JVM/container mới, chạy một phép tính nhỏ để khởi động Spark,
   rồi bắt đầu đồng hồ ngay trước khi đọc/tạo kế hoạch Silver → Gold.
5. Dừng đồng hồ sau khi ghi xong ba bảng Gold. Sau đó mới kiểm tra đầu ra.
6. Ghi thời gian, số dòng, dung lượng, kế hoạch thực thi và kết quả đối chiếu.

Các đầu ra được đối chiếu theo khóa duy nhất, mọi cột (trừ snapshot_id được
kiểm tra riêng), tolerance số thực như pipeline gốc và sort array trước khi so
sánh. Không chỉ so số dòng. Tập con dùng reference riêng; chỉ tập 100% có thể
đối chiếu trực tiếp với toàn bộ Gold đã duyệt.

## 5. Đọc kết quả

Báo cáo nằm trong `reports/hdfs/bench-matrix-.../`:

| File | Nội dung |
|---|---|
| `environment.json` | Tài nguyên Docker/host và image ID |
| `config.json` | Snapshot nguồn, thông số cố định, thứ tự chạy, hash mã benchmark |
| `storage.json` | Dung lượng đầu vào và dung lượng HDFS còn trống |
| `prepared.json` | Số dòng thực tế mỗi nguồn/tỷ lệ; kiểm tra full Gold |
| `p*-c*-r*.json` | Kết quả đo và kiểm tra của từng trial |
| `*-wall.json` | Tổng thời gian tiến trình container, gồm khởi động/kiểm tra |
| `*-explain.txt` | Kế hoạch truy vấn của ba bảng Gold |
| `summary.json` / `summary.csv` | Min, median, max, thông lượng và speedup |
| `trials.csv` | Toàn bộ số đo thô |

Chỉ dùng `workload_seconds` cho so sánh workload. `validation_seconds` và thời
gian chuẩn bị được ghi riêng/loại khỏi khoảng đo. `process_wall_seconds` không
thay thế `workload_seconds`.

Thông lượng = tổng số dòng Silver đầu vào logic / thời gian workload. Đây không
phải số dòng raw CSV hay số hàng vật lý Spark scan: Network có thể được đọc lại
cho nhiều bảng đầu ra. Giữ nguyên tác vụ này ở mọi cấu hình để so sánh nhất quán.

Speedup ở mỗi tỷ lệ = median(local[1]) / median(local[k]). Nếu local[4] chậm hơn,
giữ nguyên số đo và phân tích I/O, shuffle, overhead; không chọn bỏ lượt chậm.

Gói không dùng Spark cache/persist nhưng không xóa OS/HDFS page cache. Đây là
benchmark với trạng thái cache hệ thống không được kiểm soát, không được gọi
là cold-cache benchmark. Thay đổi kích thước có thể thay đổi số khóa và độ phủ
liên kết; các tập con dùng để đo khối lượng, không thay cho split train/test.

Chỉ kết luận ma trận hoàn tất khi `summary.json` có `status: passed`, đủ 27 trial
và từng trial có kiểm tra đầu ra đạt. Các file HDFS benchmark giữ lại để truy vết.
Không xóa dữ liệu HDFS gốc để tiết kiệm chỗ cho benchmark.

## 6. Xuất báo cáo lại / xử lý gián đoạn

```bash
python3 infra/benchmark/manage.py report
```

Lệnh lấy lượt benchmark gần nhất. Với lượt khác:

```bash
python3 infra/benchmark/manage.py report --run-id <RUN_ID>
```

ZIP chỉ chứa JSON/CSV/log/kế hoạch, không có dataset Parquet hay `.env`.
Gói chưa có resume sau tắt máy. Nếu bị ngắt, xuất báo cáo lượt dở trước; chạy lại
`smoke`/`matrix` sẽ tạo run ID mới và không dùng kết quả dở làm số đo mới.

## 7. Bật lại giao diện sau benchmark

```bash
docker compose --env-file infra/serving/.env -f infra/serving/compose.yaml up -d --wait --wait-timeout 300 postgres dashboard
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml up -d --wait --wait-timeout 300 postgres api-server scheduler dag-processor
```

Mở lại http://localhost:8501 và http://localhost:8080. Không cần chạy lại ingest,
build hoặc configure chỉ vì đã tạm dừng dịch vụ.

## 8. Kiểm thử

Xem `../RELEASE_VALIDATION.md` để phân biệt kiểm thử bản đóng gói và bằng chứng benchmark lịch sử. Test benchmark cần PySpark, PyArrow và Java theo requirements gốc; chạy từ thư mục gốc project:

```bash
python -m unittest discover -s tests -p test_benchmark.py -v
```

Không tự động chạy lại full matrix khi chỉ thay tài liệu/đóng gói.
