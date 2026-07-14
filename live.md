# Live (Phase B cadence) — Python thay dbt cho silver/gold, chạy mỗi 6h

Tài liệu này bổ sung cho `phase2.md`, thay đổi **một phần** trong đó: cách
silver/gold được cập nhật ở cadence 6h. Không đổi gì ở Phase 0/1 (raw,
bronze) và không đổi cấu trúc bảng gold — chỉ đổi **ai ghi vào bảng nào, và
bao lâu một lần**.

---

## 1. Vấn đề & quyết định

`dbt_project.yml` chỉ dùng được `materialized: table` (BigQuery sandbox
chặn mọi DML, nên các incremental strategy của dbt-bigquery — vốn compile
ra `MERGE` — đều fail). `table` = `CREATE OR REPLACE TABLE AS SELECT`, tức
là **rebuild toàn bộ** mỗi lần `dbt run`. Cadence cũ (chain `dbt run` vào
cron 6h) nghĩa là cả 8 silver + 8 gold model bị quét lại toàn bộ 4 lần/ngày
— tốn quota vô ích vì mỗi lần chỉ có thêm đúng 1 batch 6h dữ liệu mới.

**Quyết định:** tách 2 nhóm bảng theo bản chất transform, không xử lý đồng
loạt như trước:

| Nhóm | Bảng | Ai xử lý | Tần suất | Vì sao |
|---|---|---|---|---|
| Row-level | `silver_customers/orders/order_items/order_payments/order_reviews`, `fct_orders/order_items/payments/reviews` | **Python** (`06_live_transform.py`) | 6h | Mỗi dòng transform độc lập (dedup theo key, cast type, join theo order) → chỉ cần đọc đúng batch mới, APPEND, không cần đọc lại lịch sử |
| Cumulative aggregate | `dim_customers` (RFM snapshot), `mart_customer_rfm`, `mart_daily_revenue` | **dbt** (không đổi logic SQL) | 1 lần/ngày (thay vì 6h) | Recency/Frequency/Monetary phụ thuộc **toàn bộ lịch sử đơn** của khách, và `reference_date` trong `dim_customers.sql` là `MAX(order_purchase_timestamp)` toàn cục — bất kỳ đơn mới nào (của bất kỳ khách nào) cũng làm recency của MỌI khách dịch chuyển. Không thể chỉ "append" — bắt buộc phải recompute toàn bộ. Không rewrite sang Python vì sẽ tạo ra 2 nơi chứa cùng 1 logic aggregate, dễ lệch khi sửa business logic sau này. |
| Static | `dim_products/sellers/date/geolocation`, `silver_products/sellers/geolocation` | dbt, chỉ build lúc backfill | Không lặp lại | Sản phẩm/seller/geolocation **không bao giờ bị sinh giả** (replay engine tái dùng catalog thật — xem `phase1.md`), nên các bảng này không đổi sau lần backfill đầu tiên |

Vì sao nhóm "cumulative aggregate" hạ xuống 1 lần/ngày mà vẫn ổn: không có
consumer nào cần độ mới 6h cho RFM/segment — Power BI (Dashboard 1) tự
refresh theo lịch riêng, còn KMeans/repeat-purchase model train offline
trên historical data. `segment_label` (active/at_risk/lost) đổi theo
ngưỡng 90/180 ngày, không cần cập nhật trong-ngày.

## 2. File đã thêm / sửa

| File | Loại | Nội dung |
|---|---|---|
| `replay/bq_writer.py` | Sửa | Thêm `run_transform_query()` (query job ghi thẳng vào bảng đích qua `destination` + `WRITE_APPEND` — **không phải DML**, cùng cơ chế với `CREATE TABLE AS SELECT`, nên chạy được không cần billing account) và `dry_run_query()` (ước lượng bytes scan mà không ghi gì, dùng cho `--dry-run`) |
| `scripts/06_live_transform.py` | Mới | Script chính: tìm các batch (window) 6h chưa được transform, chạy SQL mirror 1:1 logic dbt cho từng bảng row-level, append vào silver/gold, rồi mới advance checkpoint riêng `live_transform_cursor` |
| `.github/workflows/live_ingest.yml` | Mới | Cron 6h: `05_live_ingest.py` → `06_live_transform.py`. **Không** còn `dbt run` ở đây nữa |
| `.github/workflows/daily_gold_refresh.yml` | Mới | Cron 1 lần/ngày (00:00 UTC): `dbt run --select dim_customers mart_customer_rfm mart_daily_revenue` + `dbt test` cùng selection |
| `phase2.md`, `backfill.yml`, `dbt_manual.yml` | Không đổi | `backfill.yml` vẫn chạy full `dbt run` một lần lúc backfill xong — đây chính là lần "seed" toàn bộ gold layer mà Python sẽ tiếp nối từ đó |

`06_live_transform.py` **không đọc lại file `.sql` của dbt** — SQL trong đó
được viết tay để khớp *chính xác* danh sách cột và logic của từng model dbt
tương ứng (đã đối chiếu từng dòng khi viết). Đây là điểm cần lưu ý ở mục 5.

## 3. Cách các bảng liên kết với nhau (quan trọng để hiểu script)

`06_live_transform.py` xử lý theo đúng thứ tự phụ thuộc, cho **mỗi batch
(window) một lần**:

0. Mốc trên (upper bound) cho việc tìm window mới **không phải đồng hồ hệ
   thống** — mà là `replay_cursor`, checkpoint thật của `05_live_ingest.py`.
   Lý do: nếu dùng `now()`, `06` có thể "thấy" 1 window đã complete theo
   lịch trước khi `05` thực sự ghi xong batch đó vào bronze (lệch vài phút
   giữa 2 lần chạy cron) → xử lý nhầm 1 batch rỗng, advance checkpoint qua
   nó, rồi bỏ sót vĩnh viễn dữ liệu thật mà `05` ghi sau đó. Dùng
   `replay_cursor` đảm bảo `06` chỉ bao giờ động vào window mà `05` đã chắc
   chắn ghi xong.
1. `silver_customers` ← lọc `bronze_customers` theo `batch_id` của batch đó
2. `silver_orders`, `silver_order_items`, `silver_order_payments`,
   `silver_order_reviews` ← tương tự, lọc theo `batch_id`
3. `fct_orders` ← join `silver_orders` (batch mới) với **toàn bộ**
   `silver_customers` (bảng dimension nhỏ, đọc full không tốn kém) để lấy
   `customer_unique_id`
4. `fct_order_items`, `fct_payments`, `fct_reviews` ← passthrough từ silver
   tương ứng, lọc theo `batch_id`

`silver_customers` không có cột `batch_id` trong output (giống hệt dbt) —
không sao, vì replay engine chỉ ghi 1 dòng `bronze_customers` **đúng 1 lần**
cho mỗi `customer_id` (khách mua lại không tạo dòng mới — xem
`replay_engine.py`), nên lọc theo batch mới là đủ, không trùng lặp.

## 4. Idempotency (chạy lại không bị trùng dòng)

Trước khi xử lý mỗi window, script kiểm tra `batch_id` đó đã có trong
`silver_orders` chưa (`bq_writer.already_loaded_batch_ids`, cùng cơ chế
`bronze` đã dùng) — nếu có, skip toàn bộ window đó (coi như đã xong) và
advance checkpoint bình thường. Checkpoint (`live_transform_cursor`) chỉ
được ghi **sau khi cả 9 bước của 1 window thành công** — nếu job bị crash
giữa chừng, lần chạy sau sẽ retry lại đúng window đó từ đầu, được bảo vệ bởi
kiểm tra `silver_orders` ở trên.

## 5. Rủi ro cần biết: logic bị duplicate ở 2 nơi

`06_live_transform.py` chứa bản SQL **viết tay** khớp với `dbt/models/silver/*.sql`
và `dbt/models/gold/fct_*.sql`. Nếu sau này sửa business logic ở 1 trong 2
nơi (ví dụ đổi công thức `is_delayed` trong `fct_orders.sql`) mà quên sửa
chỗ kia, **historical data (do dbt xử lý lúc backfill) và live data (do
Python xử lý) sẽ lệch logic** mà không có gì báo lỗi ngay.

Cách giảm rủi ro:
- Bất cứ khi nào sửa 1 trong 4 file gold `fct_*.sql` hoặc silver `*.sql`,
  luôn sửa `scripts/06_live_transform.py` cùng lúc — coi 2 chỗ này là
  "cùng 1 PR".
- Định kỳ (khuyến nghị: mỗi tuần, chạy tay) đối chiếu: chạy full
  `dbt run` (dùng `dbt_manual.yml`, không cần `--select`) rồi so sánh row
  count / vài giá trị `is_delayed`, `delivery_days` giữa kết quả dbt-rebuild
  và bảng hiện tại (do Python append) trên cùng khoảng `is_synthetic=true`
  gần nhất — nếu lệch, đó là dấu hiệu 2 nơi logic đã trôi khỏi nhau.

## 6. Setup lần đầu (migration từ pipeline cũ)

Thực hiện đúng thứ tự, **1 lần duy nhất**:

```bash
# 1) Đảm bảo đã backfill + dbt build đầy đủ (nếu chưa chạy backfill.yml
#    lần nào, chạy nó trước — nó tự chain 04_backfill.py -> dbt run -> dbt test)
#    Nếu đã chạy rồi, có thể chạy lại dbt_manual.yml (command=run, không
#    select gì) để có 1 bản full rebuild "sạch" ngay trước khi chuyển sang
#    Python — ghi lại thời điểm UTC lúc job này CHẠY XONG.

# 2) Khởi tạo checkpoint cho Python transform, dùng đúng thời điểm ở bước 1.
#    KHÔNG xử lý lại gì — chỉ đánh dấu "mọi thứ trước mốc này dbt đã lo rồi".
python scripts/06_live_transform.py --init-cursor "2026-07-14T09:00:00+00:00"

# 3) Test khô trước khi bật cron thật:
python scripts/06_live_transform.py --dry-run
#    Kiểm tra output: mỗi bảng in ra "SQL validated" + không có lỗi auth/schema.

# 4) Chạy thật 1 lần thủ công để xác nhận (nếu đã có batch mới kể từ mốc init-cursor):
python scripts/06_live_transform.py

# 5) Bật 2 workflow mới trên GitHub Actions (Settings -> Actions -> đảm bảo
#    Actions được enable, rồi push .github/workflows/live_ingest.yml và
#    daily_gold_refresh.yml lên). Cả 2 dùng chung secrets/variables đã có
#    sẵn từ Phase 1/2 (GCP_SA_KEY_JSON, GCP_PROJECT_ID, BQ_LOCATION) —
#    không cần thêm secret nào mới.
```

Sau bước 5, **không cần chạy `dbt run` (full) theo lịch nữa** —
`daily_gold_refresh.yml` chỉ chạy 3 model được chọn.

## 7. Kiểm tra tiết kiệm được bao nhiêu quota

Mỗi bước trong `06_live_transform.py` in ra số GB đã scan:

```
[bq_writer] silver_orders: appended via query job (0.0021 GB processed).
...
Live-transform run complete. 1 window(s) processed. Total: 0.0187 GB.
```

So sánh con số này với 1 lần `dbt run` full (xem trong log job trên GitHub
Actions, hoặc BigQuery Console -> Job History -> cộng `Bytes processed` của
tất cả model trong 1 lần `dbt run`) — càng về sau (dữ liệu tích lũy càng
nhiều), chênh lệch càng lớn vì `dbt run` full quét lại toàn bộ còn Python
chỉ quét đúng batch mới.

## 8. Troubleshooting

- **`[ERROR] No live_transform_cursor checkpoint found`**: chưa chạy bước
  `--init-cursor` ở mục 6. Chạy nó trước khi bật cron.
- **`already transformed, skipping` liên tục cho mọi window**: bình
  thường nếu chạy tay nhiều lần liên tiếp trong cùng 1 window 6h chưa kết
  thúc — không phải lỗi.
- **Schema mismatch / cột lạ khi append**: `run_transform_query()` đã bật
  `ALLOW_FIELD_ADDITION`, nhưng nếu đổi TÊN hoặc KIỂU 1 cột đã tồn tại
  (không phải thêm cột mới), BigQuery sẽ báo lỗi — trường hợp này phải sửa
  cả dbt SQL lẫn Python SQL rồi chạy lại `dbt_manual.yml` (full rebuild) 1
  lần để đồng bộ schema, thay vì để 2 bên tự lệch.
- **Muốn quay lại cách cũ tạm thời** (ví dụ để debug): chạy tay
  `dbt_manual.yml` với `command=run` (không select) — full rebuild vẫn hoạt
  động bình thường, không bị ảnh hưởng bởi các thay đổi ở đây.