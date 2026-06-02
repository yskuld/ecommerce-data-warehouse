-- ==========================================
-- ODS层：贴源数据加载脚本
-- 设计原则：贴源保留，不做数据清洗和业务逻辑改动
-- 加载策略：一次性全量快照（_df），模拟历史数据迁移至数仓
-- 分区字段：dt（数据日期），格式yyyy-MM-dd
-- 存储格式：ORC列式存储 + Snappy压缩
-- 数据域划分：
--   trd（交易域）：订单、订单明细、支付、评论
--   itm（商品域）：商品、品类翻译
--   crm（客户域）：顾客信息
--   sel（商家域）：商家信息
--   pub（公共域）：地理位置
-- 调度参数：${dt}由调度工具（Airflow/DolphinScheduler）在运行时动态传入
-- ==========================================


-- ==========================================
-- 交易域 - 订单表
-- 表名：ods_olist_trd_orders_df
-- 数据源：olist_orders_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_trd_orders_df
COMMENT 'ODS交易域订单表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    '${dt}' AS dt
FROM olist_orders_dataset;


-- ==========================================
-- 客户域 - 顾客信息表
-- 表名：ods_olist_crm_customers_df
-- 数据源：olist_customers_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_crm_customers_df
COMMENT 'ODS客户域顾客信息表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    '${dt}' AS dt
FROM olist_customers_dataset;


-- ==========================================
-- 交易域 - 支付表
-- 表名：ods_olist_trd_order_payments_df
-- 数据源：olist_order_payments_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_trd_order_payments_df
COMMENT 'ODS交易域支付表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    '${dt}' AS dt
FROM olist_order_payments_dataset;


-- ==========================================
-- 交易域 - 评论表
-- 表名：ods_olist_trd_order_reviews_df
-- 数据源：olist_order_reviews_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_trd_order_reviews_df
COMMENT 'ODS交易域评论表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    '${dt}' AS dt
FROM olist_order_reviews_dataset;


-- ==========================================
-- 交易域 - 订单明细表
-- 表名：ods_olist_trd_order_items_df
-- 数据源：olist_order_items_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_trd_order_items_df
COMMENT 'ODS交易域订单明细表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    '${dt}' AS dt
FROM olist_order_items_dataset;


-- ==========================================
-- 商家域 - 商家表
-- 表名：ods_olist_sel_sellers_df
-- 数据源：olist_sellers_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_sel_sellers_df
COMMENT 'ODS商家域商家表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    '${dt}' AS dt
FROM olist_sellers_dataset;


-- ==========================================
-- 公共域 - 地理位置表
-- 表名：ods_olist_pub_geolocation_df
-- 数据源：olist_geolocation_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_pub_geolocation_df
COMMENT 'ODS公共域地理位置表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    '${dt}' AS dt
FROM olist_geolocation_dataset;


-- ==========================================
-- 商品域 - 商品表
-- 表名：ods_olist_itm_products_df
-- 数据源：olist_products_dataset
-- ==========================================

CREATE TABLE IF NOT EXISTS ods_olist_itm_products_df
COMMENT 'ODS商品域商品表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY')
AS
SELECT 
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    '${dt}' AS dt
FROM olist_products_dataset;


-- ==========================================
-- 商品域 - 品类翻译表
-- 表名：ods_olist_itm_category_translation_df
-- 数据源：product_category_name_translation
-- 说明：源表无列名，因此采用先手动建表再 INSERT 的方式。
-- ==========================================

-- 1. 手动建表，明确定义列名
CREATE TABLE IF NOT EXISTS ods_olist_itm_category_translation_df
(
    category_name_pt  STRING COMMENT '葡萄牙语商品品类名',
    category_name_en  STRING COMMENT '英语商品品类名'
)
COMMENT 'ODS商品域品类翻译表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE ods_olist_itm_category_translation_df
PARTITION (dt = '${dt}')
SELECT
    col_1 AS category_name_pt,
    col_2 AS category_name_en
FROM product_category_name_translation;
