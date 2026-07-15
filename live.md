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
| `dbt/models/silver/silver_customers.sql`, `dbt/models/gold/fct_order_items.sql`, `fct_payments.sql`, `fct_reviews.sql` | Sửa | Thêm `batch_id` vào SELECT list — 4 model này vốn không có cột này (dbt không cần), nhưng `06_live_transform.py` cần nó để check idempotency per-step. Không thêm vào dbt SQL sẽ khiến `dbt run` (full rebuild) xóa mất cột này — xem mục 5 |
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

`silver_customers`, `fct_order_items`, `fct_payments`, `fct_reviews` không
có cột `batch_id` trong schema gốc do dbt tạo — script thêm cột này vào
output của cả 4 bảng (qua `ALLOW_FIELD_ADDITION`, không cần migrate schema
tay) để mọi bảng trong 9 bước đều check được idempotency, xem mục 4.

## 4. Idempotency (chạy lại không bị trùng dòng)

Mỗi bước trong 9 bước tự kiểm tra `batch_id` đó đã có trong **bảng đích của
chính nó** chưa (`bq_writer.already_loaded_batch_ids`) — nếu có thì bỏ qua
đúng bước đó, không phải toàn bộ window. Trước đây script chỉ kiểm tra 1 lần
duy nhất, dùng `silver_orders` (bước 2/9) làm đại diện cho cả window — có
lỗi: nếu job crash sau khi `silver_orders` ghi xong nhưng trước bước cuối
(`fct_reviews`), lần chạy sau sẽ thấy `silver_orders` đã có batch đó, hiểu
nhầm cả window đã xong, skip luôn — các bước chưa kịp chạy (vd. `fct_reviews`)
mất vĩnh viễn dữ liệu batch đó mà không có gì báo lỗi.

Cách hiện tại (check theo từng bước) sửa đúng lỗi này: retry sau crash sẽ tự
resume đúng từ bước còn dang dở, không mất dữ liệu và không ghi trùng vào
các bước đã thành công trước đó (`WRITE_APPEND` không tự dedupe với dữ liệu
đã có sẵn ở đích, nên đây là điều kiện bắt buộc, không chỉ là tối ưu).
Checkpoint (`live_transform_cursor`) vẫn chỉ advance sau khi `process_window()`
chạy xong toàn bộ 9 bước cho 1 window (mỗi bước hoặc chạy thật, hoặc tự xác
định là đã xong và skip).

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

## 5b. Bug đã phát hiện & sửa: đối chiếu định kỳ tự phá vỡ idempotency

Chính khuyến nghị "chạy full `dbt run` định kỳ để đối chiếu" ở mục 5 từng
tạo ra 1 nghịch lý: `already_loaded_batch_ids()` trong `bq_writer.py` check
sự tồn tại của cột `batch_id` trước — nếu cột không tồn tại, nó **im lặng
trả về rỗng** (coi như chưa có batch nào được load), không raise lỗi:

```python
if not has_column(table_id, "batch_id"):
    return set()
```

4 model dbt gốc (`silver_customers`, `fct_order_items`, `fct_payments`,
`fct_reviews`) vốn **không** select `batch_id` (dbt không cần cột này, chỉ
Python cần). Vì `materialized: table` = `CREATE OR REPLACE TABLE AS SELECT`,
mỗi lần chạy full `dbt run` để đối chiếu như mục 5 khuyên, dbt **tái tạo lại
đúng schema theo SELECT list của nó** — tức là xóa mất cột `batch_id` khỏi
đúng 4 bảng mà idempotency-check của `06_live_transform.py` cần nhất. Lần
`06` chạy kế tiếp sau đó: cả 4 bảng này đều bị coi là "chưa xử lý gì" cho
mọi batch đang trong window bị retry → append trùng dữ liệu.

**Đã sửa:** thêm `batch_id` vào SELECT của cả 4 file dbt trên (dbt chỉ
truyền lại đúng giá trị `batch_id` sẵn có từ bronze — `backfill-YYYY-MM-DD`
cho dữ liệu historical, `live-YYYY-MM-DD-HH-HH` cho dữ liệu live — không
cần biết gì thêm về ý nghĩa cột này). Từ giờ, full `dbt run` đối chiếu định
kỳ **không còn xóa mất cột tracking nữa**, mục 5 có thể làm bình thường mà
không cần thêm bước thủ công nào (không cần chạy lại `--init-cursor` sau
mỗi lần đối chiếu).

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
- **`06` báo append trùng / row count tăng bất thường sau 1 lần chạy
  `dbt_manual.yml` đối chiếu**: kiểm tra 4 file `silver_customers.sql`,
  `fct_order_items.sql`, `fct_payments.sql`, `fct_reviews.sql` có đang select
  `batch_id` không — nếu ai đó revert lại bản cũ (thiếu cột này), full dbt
  rebuild sẽ tái tạo lại bug ở mục 5b. Thêm lại cột, chạy `dbt run` full 1
  lần nữa để khôi phục cột, không cần làm gì thêm.