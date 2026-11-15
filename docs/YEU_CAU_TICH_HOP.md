# Đặc điểm nguồn và yêu cầu tích hợp dữ liệu TON IoT

Project chọn xử lý theo lô trên dữ liệu TON_IoT có sẵn. Trọng tâm là giải quyết tính không đồng nhất bằng các phương pháp tích hợp liên quan IT5427 và tạo dữ liệu phục vụ phân tích bất thường. Theo hướng dẫn giảng viên được bổ sung ngày 09/10/2026, không đặt yêu cầu mô phỏng đầu vào hay xử lý velocity. Thời gian sự kiện vẫn được dùng để căn chỉnh các quan sát lịch sử.

## Bài toán phân tích

Người phân tích cần xem lưu lượng mạng và chỉ số hiệu năng của cùng Windows host theo thời gian, đồng thời xem các luồng cảm biến có cấu trúc và kiểu giá trị khác nhau qua một mô hình quan sát chung. Cần biết nguồn nào thiếu dữ liệu hoặc không liên kết được và truy được kết quả về bản ghi gốc.

Ba câu hỏi minh họa: cửa sổ có traffic bất thường có đồng thời thay đổi CPU/memory không; các luồng cảm biến có những biến động hoặc vấn đề chất lượng nào; chuẩn hóa và kết hợp dữ liệu giúp quan sát được điều gì mà từng bảng riêng chưa cung cấp. Câu trả lời phải được đo trên dữ liệu, không giả định trước có tương quan hoặc quan hệ nhân quả.

## Đặc điểm nguồn đầu vào

Các nguồn dưới đây đều là dữ liệu có cấu trúc lưu dưới dạng CSV. Kiểu dữ liệu trong bảng là kiểu logic cần diễn giải sau parse; việc lưu CSV không làm tất cả thuộc tính có cùng ngữ nghĩa. Không gọi URI, hostname hay chuỗi trạng thái trong cột CSV là một nguồn dữ liệu phi cấu trúc riêng.

| Nguồn | Một bản ghi biểu diễn | Trường và kiểu logic tiêu biểu | Khác biệt cần xử lý |
|---|---|---|---|
| Processed Network | Một connection/flow đã trích xuất đặc trưng | ts: thời gian Unix; IP: định danh IPv4/IPv6; port: số nguyên; bytes/packets: số; proto/state: phân loại | Không cùng mức chi tiết với OS; một flow có thời lượng; có placeholder và endpoint chưa hợp lệ |
| Processed Windows 7 và 10 | Một lần lấy mẫu các performance counter | ts: Unix; CPU: số theo phần trăm; available memory: số byte; committed memory: phần trăm | Hai phiên bản đặt tên cột khác nhau; host/process/interface khác scope; không có IP trong mỗi dòng theo plan |
| IoT Weather | Một lần ghi các giá trị thời tiết | date/time: chuỗi thời gian; temperature/pressure/humidity: số; label/type: nhãn | Đơn vị cần kiểm chứng; giá trị đáng ngờ; không có khóa host để ghép trực tiếp |
| IoT Modbus | Một lần ghi nhóm giá trị thanh ghi | date/time: chuỗi; FC1–FC4: các trường số theo tên nguồn | Schema khác Weather; cần giữ đúng ngữ nghĩa tên trường, không tự suy thành function code chuẩn |
| IoT Garage Door | Một lần ghi trạng thái cửa và tín hiệu điện thoại | door_state: phân loại; sphone_signal: boolean logic nhưng nhiều cách biểu diễn | true/false/1/0 và khoảng trắng; missing không phải false; có dòng thiếu thời gian |

Phạm vi bằng chứng: cấu trúc và số đo Network file 1 đã được kiểm tra trực tiếp trong mốc 1. Đặc điểm Windows và IoT ở đây dựa vào kế hoạch được cung cấp; cần đối chiếu tiếp với CSV thực nhận. Không coi số cột, số dòng hoặc kiểu của bản tải sau là bất biến.

## Các yêu cầu tích hợp cụ thể

| Vấn đề cần giải quyết | Phương pháp và quy tắc | Đầu ra và cách kiểm tra |
|---|---|---|
| CPU Win7/Win10 khác tên nhưng có thể cùng nghĩa | Schema matching dựa tên/mô tả/scope, review thủ công; schema mapping sang host_cpu_pct | Mapping có phiên bản; nhận đúng Processor total và từ chối Process CPU |
| Các biểu diễn boolean, missing và kiểu số khác nhau | Chuẩn hóa giá trị: trim, map boolean, cast an toàn, giữ raw và quality flag | true/1 thành true, false/0 thành false; null vẫn null; đếm parse failures |
| Các schema sensor không có cùng bộ cột | Unpivot sang sensor_observation rồi UNION ALL các mapping theo GAV | Khóa event + metric; giữ sensor_stream_id và kiểu giá trị; không gộp sensor khác nhau thành cùng thiết bị |
| Windows và Network dùng cách định danh khác nhau | Tích hợp metadata topology; deterministic entity resolution theo IP đã xác nhận | entity_map có evidence; thống kê matched/unmatched; không fuzzy-match IP |
| Flow và OS không được lấy mẫu đồng nhịp | Chuẩn hóa event time; aggregate riêng từng nguồn rồi join theo host và cửa sổ | Một hàng cho mỗi host-window; coverage/missing masks; kiểm tra không nổ many-to-many |
| IoT ghi date/time khác cách biểu diễn Unix | Đối chiếu offset với ground truth của từng stream trước temporal alignment | Time rule có checksum bằng chứng; chưa rõ thời gian thì không tự ghép |
| Dữ liệu thiếu, trùng nội dung hoặc không hợp lệ | Source profiling, quarantine, phân biệt ingest duplicate với repeated measurement | input = accepted + quarantined trước unpivot; chạy lại không tăng sự kiện; vẫn giữ phép đo lặp |
| Người dùng cần truy vấn và truy vết thống nhất | Global view/materialized view, mediation query và provenance | Truy vấn CPU–traffic theo host/phút, từ window truy về file/record |

Các phương pháp tạo thành một pipeline có mục đích: đối sánh → ánh xạ → chuẩn hóa → liên kết có bằng chứng → tổng hợp và truy vấn. Không cần cài mọi thuật toán trong slide; mỗi phương pháp được chọn phải có một vấn đề dữ liệu, quy tắc thực hiện và kết quả kiểm chứng tương ứng.

## Phạm vi ghép và tiêu chí hoàn thành

Network–Windows là case tích hợp theo host khi có bằng chứng topology và khoảng thời gian giao nhau. Weather/Modbus/Garage được hợp nhất ở cấp lược đồ quan sát để truy vấn nhất quán; chưa gán chúng cho Windows host. Nếu chỉ cùng thời gian mà thiếu identity/service evidence, chỉ hiển thị làm ngữ cảnh riêng. Không bắt buộc ghép mọi nguồn vào một bảng ngang hoặc ép có quan hệ khi dataset không cung cấp bằng chứng.

Bản demo cần thể hiện được: catalog/schema từng nguồn; ví dụ trước và sau mapping/chuẩn hóa; sensor view chung; một case Network–Windows có coverage và lineage; thống kê lỗi và bản ghi không ghép được; truy vấn/phân tích giúp trả lời câu hỏi thực tế. Bảng kết quả phải phân biệt giá trị thiếu với 0 và phân biệt nhãn gốc với dự đoán.

Phương pháp đã có trong code mốc 1: mapping thủ công, normalization, unpivot, exact linkage qua topology, window aggregation, full outer join, quality accounting và provenance. Chưa có bộ đề xuất schema matching tự động; nếu bổ sung, similarity chỉ đề xuất ứng viên, scope/ngữ nghĩa vẫn cần review. Chưa có kết quả detector hoặc xác nhận Network–Windows trên CSV Windows thật.

## Điều chỉnh phạm vi công việc

Loại yêu cầu thí nghiệm velocity/replay khỏi danh sách bắt buộc. Không cần dựng bộ sinh sự kiện, Kafka hoặc Structured Streaming để đáp ứng phần yêu cầu cô vừa bổ sung. Các fixture nhỏ của unit test chỉ kiểm tra tính đúng của chương trình, không phải mô phỏng luồng dữ liệu cho project và không dùng làm kết quả thực nghiệm.

Tiếp tục triển khai theo thứ tự: lấy CSV Windows/IoT và ground truth thực tế; hoàn thiện profiling và mappings; xác nhận một case tích hợp; tạo truy vấn/phân tích và dashboard; sau đó hoàn thiện storage, điều phối và benchmark phù hợp kiến trúc đã chọn. Throughput batch có thể đo để đánh giá hiệu năng, không đổi tên nó thành độ trễ xử lý trực tuyến.
