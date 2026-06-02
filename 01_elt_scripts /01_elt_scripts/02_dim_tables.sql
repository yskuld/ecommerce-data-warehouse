-- ==========================================
-- DIM层：维度表构建脚本
-- 设计依据：星型模型 + 行业命名规范
-- 命名规范：dim_{业务分类}_{数据域}_{自定义内容}_{存储策略}
-- 业务分类：olist（Olist 电商平台）
-- 存储策略：1d（日全量快照）
-- 存储格式：ORC 列式存储 + Snappy 压缩
-- 分区策略：按 dt（数据日期）日分区
-- 包含维度表：
--   - dim_olist_itm_product_1d（商品维度）
--   - dim_olist_crm_customer_1d（顾客维度）
--   - dim_olist_sel_seller_1d（商家维度）
--   - dim_date（日期维度）
-- ==========================================


-- ==========================================
-- 商品维度表
-- 表名：dim_olist_itm_product_1d
-- 数据源：ods_olist_itm_products_df + ods_olist_itm_category_translation_df
-- 处理逻辑：关联翻译表，将葡语品类名转为英文，保留原始葡语名
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dim_olist_itm_product_1d (
    product_id              STRING  COMMENT '商品ID',
    product_category_pt     STRING  COMMENT '葡语商品分类名',
    product_category_en     STRING  COMMENT '英文商品分类名',
    product_weight_g        INT     COMMENT '商品重量（克）',
    product_length_cm       INT     COMMENT '商品长度（厘米）',
    product_height_cm       INT     COMMENT '商品高度（厘米）',
    product_width_cm        INT     COMMENT '商品宽度（厘米）'
)
COMMENT '商品维度表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dim_olist_itm_product_1d
PARTITION (dt = '${dt}')
SELECT
    p.product_id,
    p.product_category_name AS product_category_pt,
    COALESCE(t.category_name_en, p.product_category_name) AS product_category_en,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM ods_olist_itm_products_df p
LEFT JOIN ods_olist_itm_category_translation_df t
    ON p.product_category_name = t.category_name_pt
WHERE p.dt = '${dt}';


-- ==========================================
-- 顾客维度表
-- 表名：dim_olist_crm_customer_1d
-- 数据源：ods_olist_crm_customers_df
-- 处理逻辑：保留顾客ID、唯一标识、城市和州，省略邮编
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dim_olist_crm_customer_1d (
    customer_id         STRING  COMMENT '顾客ID（订单级关联）',
    customer_unique_id  STRING  COMMENT '顾客唯一标识（去重用）',
    customer_city       STRING  COMMENT '顾客所在城市',
    customer_state      STRING  COMMENT '顾客所在州'
)
COMMENT '顾客维度表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dim_olist_crm_customer_1d
PARTITION (dt = '${dt}')
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM ods_olist_crm_customers_df
WHERE dt = '${dt}';


-- ==========================================
-- 商家维度表
-- 表名：dim_olist_sel_seller_1d
-- 数据源：ods_olist_sel_sellers_df
-- 处理逻辑：保留商家ID、城市和州，省略邮编
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dim_olist_sel_seller_1d (
    seller_id       STRING  COMMENT '商家ID',
    seller_city     STRING  COMMENT '商家所在城市',
    seller_state    STRING  COMMENT '商家所在州'
)
COMMENT '商家维度表，日全量快照'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dim_olist_sel_seller_1d
PARTITION (dt = '${dt}')
SELECT
    seller_id,
    seller_city,
    seller_state
FROM ods_olist_sel_sellers_df
WHERE dt = '${dt}';


-- ==========================================
-- 日期维度表
-- 表名：dim_date
-- 生成策略：一次性生成连续日期序列，确保日历完整无缺。
-- 实现方式：使用 SPACE + SPLIT + posexplode 制造连续整数序列，
--           再通过 DATE_ADD 加到最早订单日期上，生成覆盖历史及未来可能日期。
-- 日期范围：从订单表最早日期开始，至最晚订单日期后推 20 年。
-- 说明：该表为公共维表，无需每日调度更新。
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     STRING  COMMENT '日期，格式yyyy-MM-dd',
    year        INT     COMMENT '年',
    quarter     INT     COMMENT '季度',
    month       INT     COMMENT '月',
    day         INT     COMMENT '日',
    is_weekend  INT     COMMENT '是否周末（1=是，0=否）'
)
COMMENT '日期维度表，公共维表，包含连续日历信息'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dim_date
SELECT
    date_str AS date_id,
    YEAR(date_str) AS year,
    QUARTER(date_str) AS quarter,
    MONTH(date_str) AS month,
    DAY(date_str) AS day,
    CASE 
        WHEN DAYOFWEEK(date_str) IN (1, 7) THEN 1 
        ELSE 0 
    END AS is_weekend
FROM (
    SELECT 
        DATE_ADD(min_date, days_offset) AS date_str
    FROM (
        -- 订单表最早日期作为日历起点
        SELECT MIN(DATE(order_purchase_timestamp)) AS min_date
        FROM ods_olist_trd_orders_df
    ) t1
    LATERAL VIEW posexplode(
        SPLIT(SPACE(365 * 60), '')  -- 生成 60 年的连续整数，确保覆盖范围
    ) t2 AS days_offset, dummy
) t
WHERE date_str <= DATE_ADD(
    (SELECT MAX(DATE(order_purchase_timestamp)) FROM ods_olist_trd_orders_df), 
    365 * 50  -- 最晚订单日期后推 50 年作为日历终点
);
