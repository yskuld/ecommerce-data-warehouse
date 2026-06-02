-- ==========================================
-- DWD层：增量加载示例（生产环境每日调度）
-- 说明：
--   全量初始化脚本（../03_dwd_int.sql）已完成历史数据全量加载。
--   此脚本用于接入真实业务流后的每日增量追加，由 Airflow 凌晨触发。
-- 核心变化：
--   - 源表：使用 ODS 的 _di 增量表（如 ods_olist_trd_orders_di）
--   - 写入：使用 INSERT INTO 追加当天分区，而非 INSERT OVERWRITE
--   - 其他逻辑（字段、JOIN、清洗、派生）与全量初始化完全一致
-- 调度参数：
--   ${dt}         ：目标分区日期
--   ${start_time} ：数据增量提取起始时间
--   ${end_time}   ：数据增量提取截止时间（左闭右开）
-- ==========================================

-- 示例：订单履约事实表增量加载
INSERT INTO TABLE dwd_olist_trd_ord_di
PARTITION (dt = '${dt}')
SELECT
    o.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.seller_id,
    oi.product_id,
    DATE(o.order_purchase_timestamp) AS date_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.order_approved_at,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    COUNT(oi.order_item_id) OVER(PARTITION BY o.order_id) AS item_count,
    oi.price+oi.freight_value AS total_amount,
    SUM(oi.price+oi.freight_value) OVER(PARTITION BY o.order_id) AS order_total_amount,
    DATEDIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date) AS deliver_duration,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS total_duration
FROM ods_olist_trd_orders_di AS o
JOIN ods_olist_trd_order_items_di AS oi
  ON o.order_id = oi.order_id
  AND o.order_id IS NOT NULL
  AND oi.order_item_id IS NOT NULL
  AND oi.product_id IS NOT NULL
  AND oi.seller_id IS NOT NULL
WHERE o.order_status NOT IN ('unavailable', 'canceled')
  AND oi.price IS NOT NULL
  AND oi.price >= 0
  AND oi.freight_value >= 0
  AND oi.freight_value IS NOT NULL
  AND o.order_purchase_timestamp >= '${start_time}'
  AND o.order_purchase_timestamp < '${end_time}';

-- 其他事实表（支付、评价）的增量脚本依此类推，
-- 仅需替换对应的表名和字段，逻辑结构完全一致。
