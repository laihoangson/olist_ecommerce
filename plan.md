# TỔNG QUAN DỰ ÁN: End-to-End E-commerce Data Platform (Olist)

## 1. Mục tiêu

1 project duy nhất chứng minh năng lực ở 4 vai trò: **Data Engineer, Analytics Engineer, Data Analyst, DS/ML Engineer**, dùng kiến trúc Single Source of Truth với cờ `is_synthetic` phân luồng Historical vs Live data.

---

## 2. Chiến lược dữ liệu

**Nguồn:** Olist Brazil E-commerce (Kaggle), 2016-09-04 → 2018-10-17, ~99k orders.

**Historical (`is_synthetic = false`):** giữ nguyên data gốc → dùng cho Power BI, training ML.

**Synthetic (`is_synthetic = true`):** data synthetic — 2 pha rõ ràng:

- **Phase A — Backfill (chạy 1 LẦN duy nhất, manual trigger):**  toàn bộ từ `2024-01-01` đến ngày setup hiện tại, đẩy 1 lần vào BigQuery.
- **Phase B — Live (cron 6h/lần, chạy liên tục từ sau backfill):** mỗi lần chỉ sinh và ingest đúng 1 batch (khung giờ hiện tại), tiếp nối vị trí Phase A dừng lại.

---

## 3. Kiến trúc dữ liệu (3 tầng + mart)

```
olist_raw (BigQuery dataset)
  └── Olist CSV gốc, load 1 lần, immutable, dùng làm nguồn tra cứu cho replay

olist_bronze
  └── Raw ingest từ backfill + live (Historical + Synthetic), gần như 1-1 với source

silver (dbt models)
  └── Clean, type-cast, dedup, join — CHƯA có business logic

gold (dbt models) — feature store dùng chung cho MỌI mục đích
  ├── fct_orders            -- grain: 1 order, có is_delayed, delivery_days, is_synthetic...
  ├── fct_order_items
  ├── fct_payments
  ├── fct_reviews
  ├── dim_customers          -- + RFM snapshot (recency/frequency/monetary/segment_label)
  ├── dim_sellers
  ├── dim_products
  ├── dim_date
  ├── dim_geolocation
  └── mart/
      ├── mart_daily_revenue          -- pre-agg cho Prophet + Power BI trend
      ├── mart_customer_rfm           -- RFM tính sẵn, dùng chung KMeans train/infer

```

**Nguyên tắc gold:** 1 bộ fact/dim duy nhất phục vụ tất cả — Power BI, HTML dashboard, và toàn bộ 4 model ML — để sau này có yêu cầu phân tích mới không phải sửa lại tầng transform. Cột "post-fulfillment" (delivered_date, review_score...) vẫn giữ trong fact table cho BI dùng, nhưng đánh dấu rõ trong `feature_specs/inference_time_features.md` để loại khỏi feature set khi train/serve ML (tránh leakage).

---

## 4. Vai trò & Tech Stack

| Vai trò | Trách nhiệm | Tool | Data scope |
|---|---|---|---|
| DE | Replay script, BigQuery, GitHub Actions (backfill 1 lần + live cron 6h), MERGE upsert, state management | Python, GitHub Actions, BigQuery | Historical + Synthetic |
| AE | Bronze → Silver → Gold, dbt incremental, giữ `is_synthetic` xuyên suốt | dbt, SQL | Toàn bộ pipeline transform |
| DA | Power BI: business, supply chain, customer insight | Power BI (hoặc Looker Studio nếu cần auto-refresh) | `is_synthetic = false` |
| DS/ML | Train offline, serve real-time inference | Python, Prophet/LightGBM/KMeans, FastAPI, Render.com | Train: `false` / Serve: `true` |

---

## 5. 4 Model ML

| # | Model | Thuật toán | Train scope | Lưu ý leakage |
|---|---|---|---|---|
| 1 | Revenue Forecast | Prophet | `mart_daily_revenue`, is_synthetic=false | — |
| 2 | Customer Segmentation | KMeans | `mart_customer_rfm`, chỉ dùng `recency_log` + `monetary_log` (bỏ Frequency — xem note bên dưới) | Train centroid offline, live chỉ map RFM mới vào cluster có sẵn |
| 3 | Repeat Purchase Probability | XGBoost/LogReg, `class_weight='balanced'` | `mart_repeat_purchase_training` (point-in-time: t = order completion time) | Không dùng thông tin order tương lai — xem `ml/feature_specs/inference_time_features.md` cho contract đầy đủ |
| 4 | Delivery Delay Prediction | XGBoost/LogReg | `mart_delivery_delay_training`, is_synthetic=false (point-in-time: t = order purchase time) | t sớm hơn hẳn model #3 — KHÔNG được dùng delivery_days/is_delayed/review_score của chính đơn đó làm feature, vì tại t đơn chưa giao |

**Note quan trọng (verify trên data thật):** 96.88% khách hàng Olist chỉ mua đúng 1 lần (96,096 unique customer, chỉ 2,997 mua lại). Hệ quả:
- RFM Segmentation: Frequency gần như constant → loại khỏi vector KMeans, chỉ dùng Recency + Monetary (log-transform vì cả 2 đều right-skewed nặng).
- Repeat Purchase model: positive class chỉ ~3% → bắt buộc `class_weight='balanced'`, đánh giá bằng PR-AUC/Recall chứ không dùng Accuracy.
- Target phải tính kiểu point-in-time (feature ≤ t, label nhìn tương lai) — không tính Recency kiểu "toàn bộ lịch sử" như một số tutorial tham khảo, vì cách đó leak thông tin tương lai vào feature.
- Model #3 và #4 dùng CÙNG data (fct_orders, fct_order_items...) nhưng **t khác nhau** (order completion vs order purchase) → 2 mart riêng biệt (`mart_repeat_purchase_training` vs `mart_delivery_delay_training`), không share 1 bảng feature để tránh nhầm lẫn cột nào dùng được cho model nào.

**Live serving:** Order mới từ pipeline → FastAPI chấm điểm real-time → hiển thị trên Dashboard 2 (delivery risk badge, cluster label, coupon suggestion nếu repeat-purchase probability thấp). Revenue forecast hiển thị dạng đường nét đứt nối tiếp đường thực tế trên chart.

---

## 6. Dashboard

**Dashboard 1 — Power BI (Strategic):** filter cứng `is_synthetic=false`. Trả lời: kênh thanh toán AOV cao nhất, cohort retention theo quý, heatmap seller vs customer. ⚠️ Free tier không auto-refresh — cân nhắc Looker Studio nếu cần scheduled refresh.

**Dashboard 2 — HTML/JS + FastAPI (Operations Command Center):** gọi API query `is_synthetic=true` + live data 2024. ⚠️ Render free tier cold start 30-50s — cần ping định kỳ giữ ấm.
