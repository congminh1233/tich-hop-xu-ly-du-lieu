# IT5427 — Tích hợp dữ liệu an ninh IoT để phân tích bất thường

Bản bàn giao tinh gọn từ mã nguồn nhóm cung cấp ngày 26/10/2026. Đây là hệ thống batch
TON_IoT: chuẩn hóa Network, Windows và cảm biến thành Bronze–Silver–Gold,
phân tích bất thường, đưa kết quả lên PostgreSQL/Streamlit và điều phối bằng Airflow.

**Chọn một trong hai cách dùng:**

- **Xem dashboard từ kết quả có sẵn:** mục 2; không cần `data/`, `lake/`, HDFS hay Airflow.
- **Tái tạo pipeline đầy đủ từ CSV:** mục 3–7; cần tải đúng 28 file và tạo kết quả mới trên máy triển khai.

Mã xử lý lõi được giữ nguyên từng byte để bảo toàn hash kiểm chứng. Việc tái tổ chức
chủ yếu dành cho dependency, tài liệu và công cụ đóng gói. Các file lịch sử không cần triển khai đã được bỏ khỏi bản này.
Xem [RELEASE_VALIDATION.md](RELEASE_VALIDATION.md) để biết chính xác kiểm tra đã chạy;
không coi kiểm thử tại đây là bằng chứng đã chạy Docker/full dataset trên máy mới.

## 1. Chuẩn bị và cấu trúc

Thực hiện các lệnh Bash bên dưới trong terminal **WSL Ubuntu**, ở thư mục gốc
có `toniot/`, `scripts/`, `infra/`. Mở Docker Desktop và bật WSL integration cho
đúng distro. `docker version` phải có cả Client và Server. Dùng tài khoản WSL
thường, không chạy các launcher bằng sudo. Quyền truy cập Docker socket cần
được cấu hình đúng; không dùng chmod 777.

Các phiên bản trong code: Spark 3.5.7, Hadoop 3.3.4; image HDFS dùng Java 11 và
Python 3.10; serving dùng Python 3.11; Airflow 3.1.0 dùng Python 3.12. Môi trường
WSL xử lý dữ liệu nên dùng Python 3.10, Java 11 hoặc 17 và dependencies đã pin.
Không thay các phiên bản này bằng latest trong lúc tái lập.

Máy đã chạy của nhóm có khoảng 15–16 GiB RAM dành cho WSL. Benchmark dùng driver
4 GiB, container 6 GiB và tối đa 4 CPU. Đây là cấu hình đã có bằng chứng lịch sử,
không phải cam kết về cấu hình tối thiểu. Chừa dung lượng cho raw, Bronze, các
snapshot, Docker images/volumes và mẫu benchmark. Chỉ raw + indexed Bronze của
batch trong plan đã khoảng 4 GB; tổng lưu trữ toàn hệ thống lớn hơn nhiều.
Kiểm tra ổ Windows chứa Docker VHDX, không chỉ con số dung lượng ảo HDFS.

| Đường dẫn | Vai trò |
|---|---|
| `toniot/` | Ingest, chuẩn hóa, tích hợp, báo cáo HTML |
| `config/` | Mapping host, quy tắc thời gian, danh sách và hash 28 CSV |
| `scripts/` | Evaluation, baseline, HDFS ETL và adapter Airflow |
| `infra/` | Docker và launcher HDFS, serving, Airflow, benchmark |
| `experiments/*_v1/` | Ba bộ scores đã chạy, đủ phục vụ dashboard |
| `tests/` | Tests và fixtures, không dùng fixture thay dataset |
| `tools/` | Kiểm tra CSV và đóng gói payload đã duyệt |

Source ZIP không có dataset, lake, môi trường ảo, secret, state hoặc Docker volumes.
Hai file `.env.example` chỉ giải thích biến; **không copy chúng thành `.env`**:
lệnh `configure` sẽ tự tạo secret thật và UID/GID của máy mới.

## 2. Mở dashboard từ kết quả có sẵn

Chế độ này dùng các scores đã đóng gói, không chạy lại ETL hay mô hình. Các
launcher ở bước này dùng Python chuẩn của WSL; không cần cài Spark trên WSL.

```bash
docker version
docker compose version
python3 infra/serving/manage.py configure \
  --experiment experiments/host_baseline_v1 \
  --experiment experiments/host_comparison_v1 \
  --experiment experiments/iot_baseline_v1
python3 infra/serving/manage.py up
python3 infra/serving/manage.py load
python3 infra/serving/manage.py verify
python3 infra/serving/manage.py dashboard
python3 infra/serving/manage.py status
```

Mở **http://localhost:8501**. `load` đưa scores vào PostgreSQL; `verify` kiểm tra
replay và tính nhất quán. Kết quả mong đợi là ba experiment với 16.863, 61.831,
76.929 scores tương ứng; một cửa sổ có thể có nhiều scores theo model/feature group.
Không cộng các số này thành số sự kiện raw.

Các trang tổng quan ETL có thể thiếu tài liệu mới vì `reports/` chưa được tạo:
bản này không kèm báo cáo ETL cũ để tránh nhầm với kết quả trên máy mới.
Giá trị phát hiện bất thường có hạn chế về false positive/false negative; hiển thị
được dashboard không có nghĩa mô hình phát hiện tốt.

Nếu cấu hình/volume đã có trên máy hiện tại, dùng mục 8 để resume thay vì cài
lại vào cùng stack. Compose dùng tên cố định `it5427-*`: hai bản project trên
cùng Docker daemon có thể cùng trỏ đến stack/volume; không chạy chúng đồng thời.

## 3. Chuẩn bị 28 CSV và Python cho tái tạo đầy đủ

Nguồn: https://research.unsw.edu.au/projects/toniot-datasets . Lấy processed data,
không trộn Train_Test hoặc file thử nghiệm/mirror khác byte vào batch này.

Đặt trong `data/raw/` (có thể có thư mục con):

- `Network_dataset_1.csv` đến `Network_dataset_23.csv`.
- `windows7_dataset.csv`, `windows10_dataset.csv`.
- `IoT_Weather.csv`, `IoT_Modbus.csv`, `IoT_Garage_Door.csv`.

Tên, kích thước, SHA256 và số dòng mong đợi nằm trong `config/dataset_manifest.json`,
trích từ full ETL plan đã duyệt. Giữ nguyên byte CSV, không mở rồi lưu lại bằng Excel.
Các cảm biến khác có thể được parser hỗ trợ nhưng không thuộc batch 28 file này.

```bash
mkdir -p data/raw data/ground_truth reports/hdfs reports/serving
python3 tools/check_dataset.py data/raw
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
java -version
python -c 'import pyspark, pyarrow; print(pyspark.__version__, pyarrow.__version__)'
```

Kiểm tra dataset phải `passed`, đủ 28 file. Nếu khác hash, dừng để xác minh phiên
bản dữ liệu; không sửa manifest thành hash mới để lách kiểm tra. Python 3.10 và
Java phải được cài trong WSL trước; virtualenv Windows không dùng lại được.

`config/time_rules.verified.json` giữ bằng chứng offset theo Ground Truth của
ba luồng đã kiểm tra; timezone địa lý vẫn chưa được xác định. Không cộng thêm
7 giờ trong downstream. Giả định về dòng normal được ghi ngay trong cấu hình.
Nếu dùng bộ CSV khác, phải kiểm chứng lại Ground Truth và mapping; không thừa
kế tự động kết luận của batch này. `data/ground_truth` không đi kèm release.

## 4. Tạo lake local và kiểm tra kết quả

Chạy trên thư mục release mới có `lake/` chưa tồn tại. Không ghi đè lake đang dùng.
Origin là mô tả nguồn thực tế bạn tải, không giả ghi thành nguồn mirror cũ.

```bash
python -m toniot ingest data/raw --lake lake --origin unsw_official_processed_csv
python -m toniot catalog --lake lake --output reports/catalog.json
export PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell'
python -m toniot build --lake lake \
  --time-rules config/time_rules.verified.json \
  --expected-files config/expected_files.json --strict
export IT5427_SNAPSHOT=$(python -c 'import json; print(json.load(open("lake/latest.json"))["snapshot_id"])')
python -m toniot report --lake lake --snapshot "$IT5427_SNAPSHOT" --output reports/overview.html
```

Giữ terminal này cho các bước dùng biến `IT5427_SNAPSHOT`. Mở terminal mới thì
activate venv và đọc lại biến từ `lake/latest.json`. `build` in `status=committed`,
`input_scope_complete`, source accounting và host coverage. Mở overview.html.
Catalog mới cần có đủ 28 manifest; catalog lịch sử được tải lên chỉ có một file
nên đã loại khỏi gói; phạm vi batch hiện tại lấy từ manifest 28 file.

Baseline đã nghiệm thu: Gold host 19.213, sensor 78.152, network overview 7.934;
quarantine 22.204 (21.335 Garage Door thiếu dữ liệu, 869 Network invalid IP).
Nếu số liệu khác, kiểm tra nguồn/rules/schema trước khi tiếp tục. Snapshot ID và
HDFS plan ID mới có thể khác do metadata; không hard-code ID của máy cũ.

Không có file audit standalone đầy đủ trong upload cho `quality_audit.json`:
release dùng kiểm tra trong build/evaluation/HDFS comparison.
Không có hướng dẫn chạy một `quality_audit.py` không tồn tại.

## 5. Tính lại baseline và so sánh

Các output `_rebuild` phải chưa tồn tại. Đổi tên output có chủ đích nếu chạy lại;
không xóa experiment cũ để ép lệnh thành công.

```bash
python scripts/prepare_host_evaluation.py --lake lake --snapshot "$IT5427_SNAPSHOT" \
  --output experiments/host_eval_rebuild
python scripts/run_host_baseline.py --input experiments/host_eval_rebuild \
  --output experiments/host_baseline_rebuild
python scripts/compare_host_models.py --input experiments/host_eval_rebuild \
  --output experiments/host_comparison_rebuild
python scripts/prepare_iot_evaluation.py --lake lake --snapshot "$IT5427_SNAPSHOT" \
  --output experiments/iot_eval_rebuild
python scripts/run_iot_baseline.py --input experiments/iot_eval_rebuild \
  --output experiments/iot_baseline_rebuild
```

Các thư mục scores cuối có summary.json, scores.parquet, models.json và
input_checksums.json. Nhãn được giữ riêng cho đánh giá; không đưa nhãn vào đặc
trưng phát hiện. So sánh mô hình hiện có được đánh dấu exploratory vì test đã
được xem trước đó; không diễn giải nó thành đánh giá test hoàn toàn độc lập.

## 6. Chạy HDFS ETL thật trên máy mới

Đầu vào của bước này là Bronze local đã tạo ở mục 4. HDFS ETL thực hiện lại
Bronze Parquet → Silver → Gold trên HDFS, đối chiếu với local baseline.
`infra/hdfs/manage.py up` cần Gold local nên không chạy nó trước mục 4.

```bash
python3 infra/hdfs/manage.py up --snapshot "$IT5427_SNAPSHOT"
python3 infra/hdfs/manage.py verify --snapshot "$IT5427_SNAPSHOT"
python3 scripts/run_hdfs_etl.py smoke --snapshot "$IT5427_SNAPSHOT"
python3 scripts/run_hdfs_etl.py full --snapshot "$IT5427_SNAPSHOT"
```

Lần đầu build image tải Hadoop khoảng 663 MB, có thể lâu tùy mạng. `verify` dùng
job roundtrip đã sửa; không cần chạy installer patch.
Smoke phải passed trước full. Full kết thúc với `PASSED HDFS ETL full` và ZIP
`reports/hdfs/etl-full-...zip`. Trong thư mục report, summary phải passed/full,
comparison passed và fsck HEALTHY. Ghi tên report full mới để dùng ở mục 7.
Mở NameNode tại http://localhost:9870 .

## 7. Serving và Airflow trên kết quả mới

Nếu đã thử dashboard với scores có sẵn, có thể cấu hình chọn scores `_rebuild`.
Không thay config trong khi DAG đang chạy. Sau khi chọn experiment mới cần cấu
hình lại bridge vì bridge kiểm tra hash config.

```bash
python3 infra/serving/manage.py configure \
  --experiment experiments/host_baseline_rebuild \
  --experiment experiments/host_comparison_rebuild \
  --experiment experiments/iot_baseline_rebuild
python3 infra/serving/manage.py up
python3 infra/serving/manage.py load
python3 infra/serving/manage.py verify
python3 infra/serving/manage.py dashboard
```

Thay giá trị mẫu dưới đây bằng basename của report **full mới passed** ở mục 6:

```bash
export IT5427_APPROVED_REPORT='etl-full-THAY_BANG_TEN_REPORT_MOI'
python3 infra/airflow/manage.py configure --approved-report "$IT5427_APPROVED_REPORT"
python3 infra/airflow/manage.py up
python3 infra/airflow/manage.py login
python3 infra/airflow/manage.py status
python3 infra/airflow_serving/manage.py configure
python3 infra/airflow_serving/manage.py status
python3 infra/airflow_serving/manage.py trigger
```

`configure` phát hiện mount/network/image của máy mới và tạo credential mới;
không copy runtime.json/serving_bridge.json hoặc image SHA của máy cũ. Đợi DAG
được phát hiện rồi trigger. Mở http://localhost:8080, dùng tài khoản từ `login`.
Không gửi output `login` lên nơi công khai.

DAG `toniot_batch_serving` có tám task: plan, upload, reference, build, compare,
finalize, publish_postgres, verify_postgres. Chờ tất cả success rồi xuất kết quả:

```bash
python3 infra/airflow_serving/manage.py report
```

Thử retry chỉ khi không còn DAG đang chạy:

```bash
python3 infra/airflow_serving/manage.py trigger --fail-serving-once
```

Lần lỗi chủ đích xảy ra trước kết nối database; retry thành công không chứng
minh khả năng phục hồi mọi lỗi giữa transaction. DAG hiện replay batch đã duyệt
và publish scores đã tính, chưa tự ingest mọi batch mới/huấn luyện lại/backfill.
Chạy lại build có thể tái sử dụng snapshot đã committed, không phải fresh ETL.

## 8. Tiếp tục sau tắt máy, và dừng an toàn

Mở Docker Desktop, vào đúng project WSL. Chỉ dùng khi stack đã cài và `.env`,
runtime, volumes vẫn còn; không dùng đoạn này thay first-time configure.

```bash
docker compose -f infra/hdfs/compose.yaml up -d --wait --wait-timeout 300 namenode datanode
docker compose --env-file infra/serving/.env -f infra/serving/compose.yaml up -d --wait --wait-timeout 300 postgres dashboard
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml up -d --wait --wait-timeout 300 postgres api-server scheduler dag-processor
python3 infra/airflow_serving/manage.py status
```

Với chế độ dashboard-only chỉ cần dòng serving. Sau crash, kiểm tra DAG run cũ
trước khi trigger mới. Nếu host có DAG/config nhưng container thấy thư mục rỗng,
kiểm tra mount và WSL integration; khi không có run đang hoạt động có thể recreate
api-server/scheduler/dag-processor bằng `up -d --force-recreate --no-deps --no-build
--pull never --wait --wait-timeout 300` với cùng compose/env-file. Không xóa volume.

Dừng sau khi task đã xong:

```bash
python3 infra/airflow/manage.py stop
python3 infra/serving/manage.py stop
python3 infra/hdfs/manage.py stop
```

Không dùng `down -v`. Thiếu pyspark: kiểm tra đang activate đúng venv WSL.
Exit 124 là timeout, cần đọc log/task đang chờ; không mặc định tăng RAM/timeout.
Hash mismatch: kiểm tra code/config/image có thay đổi, tạo lại approval/run hợp
lệ nếu cần; không sửa checksum trong receipt cũ hoặc bỏ validation.

## 9. Benchmark và đóng gói

Benchmark cần full HDFS snapshot và runtime Airflow đã configure. Sau khi DAG
hoàn tất, dừng Airflow/serving, giữ HDFS; chạy:

```bash
python3 infra/benchmark/manage.py check
python3 infra/benchmark/manage.py smoke
python3 infra/benchmark/manage.py matrix
```

Chỉ chạy matrix sau smoke passed. Có 27 trial, 25/50/100% Silver × local[1/2/4]
× ba lần. `local[k]` là k luồng trên một máy. Thời gian workload gồm đọc Silver,
tổng hợp/join và ghi Gold; không gồm ingest, Bronze→Silver, model, Airflow hay
validation. Cache OS/HDFS không được xóa. Chi tiết ở [docs/BENCHMARK.md](docs/BENCHMARK.md).

Đóng gói lại đúng payload đã duyệt, bỏ mọi runtime phát sinh:

```bash
python3 tools/package_release.py --dry-run --output ../IT5427_repacked.zip
python3 tools/package_release.py --output ../IT5427_repacked.zip
```

Manifest SHA256 kiểm tra mọi file payload. Công cụ từ chối file thay đổi/mất,
symlink hoặc ZIP đích đã tồn tại. Nếu có sửa code, cần một bản release và bằng
chứng kiểm tra mới; không đổi manifest chỉ để che sai khác.

## 10. Các file cài đặt cần dùng

WSL chỉ cần `requirements.txt` ở gốc. `infra/serving/requirements.txt` được Dockerfile serving cài trong container; Airflow cài dependencies trong Dockerfile riêng. Không cài Airflow vào venv ETL.

Tests và fixtures được giữ để kiểm tra thay đổi sau này, không phải dữ liệu dùng để trình diễn kết quả nghiên cứu. Chi tiết tinh gọn ở [CHANGELOG.md](CHANGELOG.md).
