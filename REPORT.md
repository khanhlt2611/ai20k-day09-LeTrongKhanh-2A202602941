# REPORT — Lab Day 9

> Báo cáo dùng số liệu từ các lần chạy và bằng chứng trong repo. Ngưỡng chuẩn giữ nguyên; các kết luận chưa có benchmark được ghi rõ là nhận định.

| | |
|---|---|
| Họ và tên | Lê Trọng Khánh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602941` |
| GitHub | `khanhlt2611` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07900` |

## Kết quả cuối

- `dataset_version` của bạn: `a3eff0b109cd`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **671 / 154 / 194** (tổng **1.019**); tương ứng **18 / 4 / 4 trip**.
- [report/verify.txt](report/verify.txt): **27/27 checks passed**; unit test **9/9 đạt**.
- Ảnh trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_failure.png`, `airflow_failed_graph.png`, `airflow_recovery.png`.
- Gold có **436 frame A**, **583 frame B**; snapshot manifest và lịch sử Airflow được lưu bổ sung trong `report/evidence/`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

A có `ts_utc` dạng ISO UTC, B có `timestamp_ms` dạng epoch ms; A dùng `speed_mps` (m/s), B dùng `spd_kmh` (km/h); hướng là `heading_deg` so với `hdg`; GPS là `lat/lon/alt_m` so với `latitude/longitude/altitude`. Bàn đạp A ở tỷ lệ 0–1 (`gas`, `brake`), B ở phần trăm (`throttle_pct`, `brake_pct`); B còn có đồng hồ chạy trước video **1.500 ms**, A không lệch. Bronze giữ **3.021 dòng A**, **3.234 dòng B**. Mỗi nguồn một bảng tránh gộp sai schema/đơn vị; giữ giá trị dưới dạng chữ, kể cả ô trống, giúp truy nguyên và áp dụng lại quy tắc làm sạch mà không mất thông tin gốc. `_batch_id`, `_row_no` giữ lineage; SHA-256 giúp nhận diện file khi ingest lại.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

A được cast timestamp ISO sang UTC. B trừ **1.500 ms** trước khi đổi epoch ms sang timestamp, chia tốc độ cho **3,6**, chia bàn đạp phần trăm cho **100**, rồi ánh xạ tên cột về schema chung. Verify ghi mẫu đầu B là **2025-10-08 14:30:00.009 UTC**, sau đầu video chỉ **9 ms**, chứng minh độ lệch đã được trừ.

| Drive | Duplicate | Speed missing | Speed out of range | Tổng rejects | Sạch | Nhận về |
|---|---:|---:|---:|---:|---:|---:|
| `drive_a` | 20 | 30 | 3 | 53 | 2.968 | 3.021 |
| `drive_b` | 20 | 30 | 3 | 53 | 3.181 | 3.234 |

Sạch cộng rejects bằng đầu vào ở cả hai drive; tốc độ sạch nằm trong 0–70 m/s, không trùng `(drive_id, ts)`. Lưu dòng lỗi cùng lý do trong quarantine giúp kiểm toán, đo chất lượng nguồn và xử lý lại khi sửa luật. Xóa chúng ngay sẽ mất bằng chứng lỗi và khó giải thích chênh lệch số dòng. Số liệu: [run_summary.json](report/evidence/run_summary.json), [verify.txt](report/verify.txt).

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` báo khoảng cách mẫu vượt **1.000 ms** nhưng là `warn`, vì cảm biến mất tín hiệu là lỗi đã biết; frame không đồng bộ trong **150 ms** vẫn bị chặn khỏi Gold bằng `sync_ok`. `in_range` là `error` vì tốc độ ngoài **0–70 m/s** không được phép đi tiếp. [break_gate.json](report/evidence/break_gate.json) ghi khi nới luật lọc lên **1.000 m/s**, test **`in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng**, tổng kết `PASS=5 WARN=1 ERROR=1 SKIP=0`. Khôi phục luật gốc cho `PASS=6 WARN=1 ERROR=0 SKIP=0`. Cổng kiểm tra độc lập vẫn chặn được dữ liệu sai khi luật làm sạch upstream bị nới.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

[idempotency.json](report/evidence/idempotency.json) dùng `trip-00003`: trước và sau **hai lần chạy lại** đều có **1.246 dòng**, fingerprint đều là **`6c7480dfde7e`**. Trip này có **60 frame**; append hai lần vào bảng thử tạo **120 dòng**, tức nhân đôi. Pipeline overwrite theo predicate `trip_id` thay đúng partition thay vì thêm bản sao. Delta ghi dữ liệu rồi công bố bằng commit, nên người đọc thấy snapshot đã commit; tránh khoảng trống do xóa trước rồi ghi lại. Nếu lỗi trước commit, snapshot cũ vẫn là dữ liệu nhìn thấy. Đây là tính an toàn của việc ghi bảng Delta trong lab, không có nghĩa mọi tác động bên ngoài bảng đều chung một transaction.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Frame đen có độ sáng trung bình **<12**, bị loại khỏi Gold; frame tối có độ sáng **từ 12 đến dưới 40**, được gắn `is_dark` nhưng không bị loại chỉ vì tối. A có **6 frame đen**, B có **4**; B có **61 frame mang cờ tối**. Giữ điều kiện thiếu sáng là hợp lý để dataset đại diện cả đường hầm/bóng râm. Frame tối vẫn phải vượt kiểm tra độ nét, trùng và đồng bộ; số frame mang cờ tối không đồng nghĩa tất cả vào Gold. Theo [threshold.json](report/evidence/threshold.json), tôi chọn ngưỡng **2**: ngưỡng 0 dễ giữ ảnh gần như trùng có nhiễu nhỏ; ngưỡng 6 loại **363 frame A, 244 frame B**, có thể bỏ cả thay đổi cảnh thật. Xe chậm thường tạo ảnh liền nhau giống hơn, nên cần xem tốc độ cùng nội dung ảnh khi chọn ngưỡng. Lab chưa đo chất lượng mô hình để kết luận 2 tối ưu trong mọi tình huống.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

[leakage.json](report/evidence/leakage.json) ghi **1.019 frame thuộc 26 trip**. Chia theo frame làm **26/26 trip nằm ở nhiều split**, và cả 26 đều có frame ở train lẫn test; chia theo trip của Gold có **0 trip bị chia cắt**. Frame gần nhau cùng cảnh có thể khiến mô hình gặp ảnh rất giống train khi đánh giá test, làm điểm kiểm thử cao giả tạo và không phản ánh khả năng tổng quát sang chuyến mới. Lab chứng minh nguy cơ leakage qua cấu trúc split; chưa huấn luyện mô hình để đo mức tăng accuracy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Code sắp theo `frame_id`, ghép từng cặp **`frame_id|split`**, rồi lấy **12 ký tự đầu SHA-256** làm version. Cùng danh sách frame và split cho cùng version, không phụ thuộc giờ chạy hoặc số commit Delta. Lần chạy toàn bộ ngày **12/10** và DAG phục hồi ngày **13/10** đều cho **`a3eff0b109cd`**, với **671 / 154 / 194 frame**. MSSV được trộn vào `md5(trip_id || LAB_STUDENT_ID)` để xếp trip trong từng drive; thay MSSV có thể đổi split và version. Dataset dùng nguyên **`2A202602941`**; chỉ bài tập chọn trip dùng **`202602941 % 26 = 21`**, bỏ tiền tố `2A` theo cách chọn đã dùng trong bài. Giới hạn: version hiện băm **ID và split**, không băm pixel ảnh hay toàn bộ telemetry, nên phải đọc cùng SHA-256 đầu vào, manifest và lineage để kiểm chứng nội dung đầy đủ.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Dataset lấy từ hai video `drive_a.mp4`, `drive_b.mp4`, hai CSV telemetry và `trips.csv` của gói lab L2D-v3, đã dựng lại timeline; pipeline lấy mẫu **2 frame/giây**. [Manifest thực tế](report/evidence/dataset_manifest.json) ghi các version Delta sau:

| Bảng upstream | Version |
|---|---:|
| `bronze.raw_telemetry_a` | 0 |
| `bronze.raw_telemetry_b` | 0 |
| `bronze.trips_raw` | 0 |
| `bronze.video_files` | 1 |
| `silver.frames` | 25 |
| `silver.telemetry` | 0 |
| `silver.frames_flagged` | 0 |
| `silver.frame_telemetry` | 0 |

SHA-256 video A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Manifest ghi luật bỏ frame đen, mờ, trùng, mất đồng bộ và chia theo trip/phân tầng theo drive; `manifest_sha256` là `2d33084250c0055893fe2dc7efb5418b05f937b93881391f6ce1308fff9fe3aa`. Tái tạo sau ba tháng cần giữ dữ liệu gốc khớp [MANIFEST đầu vào](data/MANIFEST.json), snapshot Delta hoặc khả năng dựng lại chúng, mã/dependencies trong Git, MSSV và cấu hình ngưỡng/split. Chỉ ngày tạo hoặc chuỗi version không đủ tái tạo đúng dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Tôi đặt `LAB_FAIL_ONCE=trip-07900`, chạy reset xong mới trigger DAG. Trong lần chạy **13/10/2026 lúc 09:30:52 giờ Việt Nam** (`02:30:52 UTC`), đúng **1 task `silver_frames` index 21 failed**, **7 task downstream upstream_failed**. [Log lỗi](report/screenshots/airflow_failure.png) có `simulated crash while extracting trip-07900`. Tôi Clear task con index 21 và chọn Downstream để giải phóng 7 task bị chặn, không Clear 25 task frame đã thành công. [Lịch sử Airflow](report/evidence/airflow_recovery.json) xác nhận index 21 thành công ở `try_number=2`, 25 task frame khác ở `try_number=1`, DAG có một lần Clear và kết thúc success. Sau phục hồi vẫn **1.246 frame Silver**, **1.019 frame Gold**, **27/27 PASS**, version **`a3eff0b109cd`**; không nhân đôi. [Graph phục hồi](report/screenshots/airflow_recovery.png) là ảnh đúng lần chạy này. Sau bài tập đã bỏ `LAB_FAIL_ONCE` và giữ nguyên MSSV.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Đây là dự đoán từ cấu hình lab, chưa phải benchmark 500 xe/ngày. Ba giới hạn đáng ưu tiên kiểm tra:

1. **Decode/điều phối đơn máy:** FFmpeg xử lý video, `silver_frames` giới hạn một task hoạt động trong DAG run, LocalExecutor chỉ có parallelism 2. Tôi sẽ tách worker theo trip/xe sang cụm Ray hoặc worker container, cấp CPU/GPU theo tải và giữ khóa ghi partition. [Ray hỗ trợ đọc video thành frame, điều chỉnh concurrency và tài nguyên](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_videos.html).
2. **ELT DuckDB đơn máy:** join frame–telemetry, sắp xếp và rebuild bảng sẽ giới hạn bởi RAM, CPU và I/O. Tôi sẽ dùng Spark cho ELT phân tán, xử lý tăng dần theo ngày/xe thay vì rebuild toàn bộ, giữ test và quarantine. [Spark phân phối công việc tới executor trên các node](https://spark.apache.org/docs/latest/cluster-overview.html).
3. **Object storage/ổ đĩa cục bộ:** MinIO đơn container, nhiều JPEG nhỏ và copy ảnh Silver sang Gold làm tăng I/O/dung lượng. Tôi sẽ dùng object storage có khả năng mở rộng, partition ngày/xe, gộp dữ liệu theo lô và cân nhắc manifest tham chiếu ảnh bất biến thay vì nhiều bản sao. Cần đo throughput, dung lượng và thời gian chờ trước khi chọn quy mô triển khai.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu hỏi:** 5 trip nào có tỷ lệ khung hình bị loại cao nhất, và tốc độ trung bình của các trip đó là bao nhiêu?

SQL đã lưu trên Metabase được chép vào [metabase_query.sql](report/metabase_query.sql):

```sql
SELECT
    drive_id, trip_id, frames, frames_kept,
    frames - frames_kept AS frames_dropped,
    ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct,
    avg_speed_kmh
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, trip_id
LIMIT 5;
```

| Trip | Drive | Tổng | Giữ | Loại | Tỷ lệ loại | Tốc độ TB, km/h |
|---|---|---:|---:|---:|---:|---:|
| `trip-04622` | A | 60 | 26 | 34 | 56,67% | 67,1 |
| `trip-03904` | A | 22 | 11 | 11 | 50,00% | 46,1 |
| `trip-02086` | A | 60 | 32 | 28 | 46,67% | 17,6 |
| `trip-08888` | A | 60 | 36 | 24 | 40,00% | 6,5 |
| `trip-07774` | A | 34 | 21 | 13 | 38,24% | 12,8 |

`trip-04622` cần kiểm tra trước vì tỷ lệ loại cao nhất, **34/60 = 56,67%**. `frames_dropped` đếm frame không vào Gold, không cộng các cờ chất lượng có thể chồng lấp. Top 5 đều thuộc A nhưng có trip chạy nhanh lẫn chậm; bảng này chưa chứng minh tốc độ là nguyên nhân loại, cần xem cờ chất lượng và ảnh của trip. [Ảnh dashboard có câu hỏi riêng](report/screenshots/metabase_dashboard.png).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều đáng chú ý là frame tối được gắn cờ riêng, còn chia ngẫu nhiên theo frame làm cả 26 trip rò rỉ dù tỷ lệ train/test nhìn có vẻ hợp lý. Quá trình chạy trên Windows cho thấy CRLF của script và UTF-16 của log có thể cản trở vận hành/checker dù thuật toán không đổi. Phục hồi xác nhận chỉ cần chạy lại partition lỗi. Tôi vẫn cần thêm kiểm chứng bằng huấn luyện để chọn ngưỡng dHash. Lỗi chưa được kiểm tra đầy đủ là GPS nhảy bất thường hoặc góc lái sai khi tốc độ vẫn hợp lệ; các test hiện tập trung vào tốc độ, trùng dòng, ảnh và đồng bộ. Version ID/split cũng cần đọc cùng manifest và SHA-256 đầu vào.
