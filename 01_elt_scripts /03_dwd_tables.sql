-- ==========================================
-- DWD层：事实表构建脚本（全量初始化）
-- 设计依据：业务总线矩阵 + 维度建模（星型模型）
-- 包含事实表：
--   - dwd_olist_trd_ord_di（订单履约，累积快照）
--   - dwd_olist_trd_pay_di（订单支付，事务）
--   - dwd_olist_trd_rev_di（商品评价，事务）
--
-- 加载策略说明：
--   1. 全量初始化（当前脚本）：
--      从 ODS 的 _df 表（一次性全量快照）加载全部历史数据，
--      使用 INSERT OVERWRITE PARTITION + 时间戳过滤参数模拟增量写入。
--   2. 增量追加（生产环境切换，见 daily/dwd_incremental_example.sql）：
--      接入真实业务流后，ODS 层提供 _di 增量表，
--      DWD 层改为从 ODS _di 表读取每日增量数据，
--      通过 INSERT INTO PARTITION 实现增量追加写入。
--
-- 技术栈：Hive SQL / 维度建模 / 星型模型
-- 存储格式：ORC 列式存储 + Snappy 压缩
-- 分区策略：按 dt（数据日期）日分区
-- ==========================================


-- ==========================================
-- [订单履约事实表] [dwd_olist_trd_ord_di]
-- 类型：累积快照事实表
-- 粒度：订单-商品-商家（一行 = 一个订单内一个商家提供的一个商品）
-- 联合主键：order_id + order_item_id
-- 来源：ods_olist_trd_orders_df + ods_olist_trd_order_items_df
-- 数据范围：仅包含有效履约订单，状态为 created / approved / invoiced /
shipped / delivered
-- 排除逻辑：canceled（已取消）和 unavailable（缺货/不可用）的订单不进入
此表，因其未完成履约生命周期，会产生大量空指标
-- 数据质量与特殊处理：
--   1. 所有主键外键强制 NOT NULL，防止关联断裂
--   2. price 和 freight_value 过滤 NULL 与负数，保证度量值可用
--   3. 派生 item_count（订单商品件数）和 order_total_amount（订单总金额）
使用窗口函数在明细粒度上附加订单级别汇总
--   4. deliver_duration 衡量承运耗时，total_duration 衡量全流程耗时
--   5. 保留多个业务时间戳（审核通过、交付快递、客户签收等），支撑
订单流转时效分析
-- 扩展规划：如需分析取消率和缺货率，将构建订单全生命周期事实表
dwd_olist_trd_ord_status_df
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dwd_olist_trd_ord_di (
    order_id STRING COMMENT '订单ID',
    order_item_id INT COMMENT '订单商品序号',
    customer_id STRING COMMENT '顾客ID',
    seller_id STRING COMMENT '商家ID',
    product_id STRING COMMENT '商品ID', 
    date_id STRING COMMENT '下单日期',
    order_status STRING COMMENT '订单状态',
    order_purchase_timestamp TIMESTAMP COMMENT '下单时间',
    order_delivered_carrier_date TIMESTAMP COMMENT '交付快递时间',
    order_delivered_customer_date TIMESTAMP COMMENT '客户签收时间',
    order_estimated_delivery_date TIMESTAMP COMMENT '预计送达时间',
    order_approved_at TIMESTAMP COMMENT '审核通过时间',
    shipping_limit_date TIMESTAMP COMMENT '承诺最晚发货日',
    price DECIMAL(10,2) COMMENT '商品单价',
    freight_value DECIMAL(10,2) COMMENT '商品运费',
    item_count INT COMMENT '订单内商品件数',
    total_amount DECIMAL(10,2) COMMENT '商品总金额',
    order_total_amount DECIMAL(10,2) COMMENT '订单总金额',
    deliver_duration INT COMMENT '承运时间（天）',
    total_duration INT COMMENT '订单完成总时长（天）'
)
COMMENT '订单履约事实表，粒度：订单-商品-商家'
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 2. 加载数据（全量初始化）
INSERT OVERWRITE TABLE dwd_olist_trd_ord_di
PARTITION (dt = '${dt}')
SELECT
    -- 联合主键
    o.order_id,
    oi.order_item_id,
    -- 维度外键
    o.customer_id,
    oi.seller_id,
    oi.product_id,
    DATE(o.order_purchase_timestamp) AS date_id,
    -- 退化维度
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.order_approved_at,
    oi.shipping_limit_date,
    -- 原始度量值
    oi.price,
    oi.freight_value,
    -- 派生度量值
    COUNT(oi.order_item_id) OVER(PARTITION BY o.order_id) AS item_count,
    oi.price+oi.freight_value AS total_amount,
    SUM(oi.price+oi.freight_value) OVER(PARTITION BY o.order_id) AS order_total_amount,
    DATEDIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date) AS deliver_duration,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS total_duration
FROM ods_olist_trd_orders_df AS o
JOIN ods_olist_trd_order_items_df AS oi
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
  
-- ==========================================
-- [订单支付事实表] [dwd_olist_trd_pay_di]
-- 类型：事务事实表
-- 粒度：一笔支付流水（一行 = 一次支付动作）
-- 联合主键：order_id + payment_sequential
-- 来源：ods_olist_trd_order_payments_df + ods_olist_trd_orders_df
-- 数据质量与特殊处理：
--   1. payment_installments 存在约 2 条取值为 0 的记录，且支付方式并非全部为 not_defined，初步排查为早期数据不规范或银行交易信息回传缺失导致
--   2. 在 DWD 层 ETL 中通过 CASE WHEN payment_installments = 0 THEN 1
 END 将 0 统一修正为 1（一次性付清），保证统计一致性
--   3. is_installment 基于清洗后的分期数判断（>1 为分期），不受异常值影响
--   4. payment_sequential 为支付流水序号（同一订单可能有多笔资金记录，如 voucher 逐张核销），通过 MAX() OVER() 派生 payment_attempts，
--      反映该订单的支付流水总笔数
--   5. payment_value 过滤 NULL 与负数，保证金额可统计
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dwd_olist_trd_pay_di (
    order_id STRING COMMENT '订单ID',
    payment_sequential INT COMMENT '支付序号（联合主键）',
    customer_id STRING COMMENT '顾客ID',
    date_id STRING COMMENT '支付日期',
    payment_type STRING COMMENT '支付方式',
    payment_installments INT COMMENT '分期期数',
    payment_value DECIMAL(10,2) COMMENT '支付金额',
    payment_attempts INT COMMENT '支付流水笔数',
    is_installment INT COMMENT '是否分期（1=分期，0=未分期）'
)
COMMENT '订单支付事实表，粒度：一笔支付流水'
PARTITIONED BY (dt STRING COMMENT '数据日期') 
STORED AS ORC 
TBLPROPERTIES ('orc.compress'='SNAPPY'); 

-- 2. 加载数据（全量初始化）
INSERT OVERWRITE TABLE dwd_olist_trd_pay_di
PARTITION (dt = '${dt}')
SELECT
    -- 联合主键
    op.order_id,
    op.payment_sequential,
    -- 维度外键
    o.customer_id,
    DATE(o.order_purchase_timestamp) AS date_id,
    -- 退化维度
    op.payment_type,
    (CASE 
      WHEN op.payment_installments = 0 THEN 1
      ELSE op.payment_installments 
     END) AS payment_installments,
    -- 原始度量值
    op.payment_value,
    -- 派生度量值
    MAX(op.payment_sequential) OVER(PARTITION BY op.order_id) AS payment_attempts,
    CASE 
        WHEN op.payment_installments > 1 THEN 1 
        WHEN op.payment_installments = 0 THEN 0 
        ELSE 0 
    END AS is_installment
FROM ods_olist_trd_order_payments_df AS op
JOIN ods_olist_trd_orders_df AS o
  ON op.order_id = o.order_id
  AND op.order_id IS NOT NULL
  AND op.payment_sequential IS NOT NULL
  AND o.customer_id IS NOT NULL
WHERE op.payment_installments IS NOT NULL
  AND op.payment_value IS NOT NULL
  AND op.payment_value >= 0
  AND op.payment_sequential >= 1
  AND o.order_purchase_timestamp >= '${start_time}'
  AND o.order_purchase_timestamp < '${end_time}';
  
-- ==========================================
-- [商品评价事实表] [dwd_olist_trd_rev_di]
-- 类型：事务事实表
-- 粒度：一条评价记录（一行 = 一次评价）
-- 联合主键：order_id + review_id
-- 来源：ods_olist_trd_order_reviews_df + ods_olist_trd_orders_df
-- 数据质量与特殊处理：
--   1. review_score 在加载时强制过滤 NULL 和范围外取值（1-5分），保证
评分数据完整性
--   2. score_bucket 将评分映射为 1（差评，1-2分）、2（中评，3分）、
3（好评，4-5分），用于分组观察用户评分分布
--   3. is_good_review 直接从评分派生（>=4 为好评），便于快速计算好评率，
与 score_bucket 分工清晰：前者用于分组，后者用于指标计算
--   4. review_comment_message 完整保留评论文本，为后续接入 NLP 情感分析
和关键词提取预留
--   5. has_comment 区分有/无文本评价，comment_count 统计字符数以衡量
评论内容深度，两者分工明确
--   6. days_to_review 采用 DATEDIFF 按自然天数计算，分析用户收货后评价
行为的时间分布
--   7. is_reply 标记商家是否回复，hours_to_reply 采用
TIMESTAMPDIFF(MINUTE)/60 精确到小时，用于客服效率监测
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dwd_olist_trd_rev_di (
   
  order_id STRING COMMENT '订单ID',
  review_id STRING COMMENT '评价ID',

  customer_id STRING COMMENT '顾客ID',
  date_id STRING COMMENT '评价日期',

  review_comment_message STRING COMMENT '评价详情',
  order_delivered_customer_date TIMESTAMP COMMENT '客户签收时间',
  review_creation_date TIMESTAMP COMMENT '评价创建时间',
  review_answer_timestamp TIMESTAMP COMMENT '商家答复时间',

  review_score INT COMMENT '商品打分',

  score_bucket INT COMMENT '评价分级（1=差评，2=中评，3=好评）',
  is_good_review INT COMMENT '是否好评（1=好评，0=非好评）',
  days_to_review INT COMMENT '评价间隔（天）',
  is_reply INT COMMENT '是否答复（1=答复，0=未答复）',
  hours_to_reply INT COMMENT '答复间隔（小时）',
  comment_count INT COMMENT '评价字符数',
  has_comment INT COMMENT '是否有评价文本（1=有文本，0=无文本）'
)
COMMENT '商品评价事实表，粒度：订单-评价'
PARTITIONED BY (dt STRING COMMENT '数据日期') 
STORED AS ORC 
TBLPROPERTIES ('orc.compress'='SNAPPY'); 

-- 2. 加载数据（全量初始化）
INSERT OVERWRITE TABLE dwd_olist_trd_rev_di
PARTITION (dt = '${dt}')
SELECT
  --联合主键
  ore.order_id,
  ore.review_id,
  --维度外键
  o.customer_id,
  DATE(ore.review_creation_date) AS date_id,
  --退化维度
  ore.review_comment_message,
  o.order_delivered_customer_date,
  ore.review_creation_date,
  ore.review_answer_timestamp,
  --原生度量值
  ore.review_score,
  --派生度量值
  CASE 
      WHEN ore.review_score <=2 THEN 1 
      WHEN ore.review_score=3 THEN 2  
      ELSE 3 
  END AS score_bucket,
  CASE 
      WHEN ore.review_score >= 4 THEN 1 
      ELSE 0 
  END AS is_good_review,
  DATEDIFF(ore.review_creation_date, o.order_delivered_customer_date) AS days_to_review,
  CASE 
      WHEN ore.review_answer_timestamp IS NOT NULL THEN 1 
      ELSE 0 
  END AS is_reply,
  ROUND(TIMESTAMPDIFF(MINUTE,ore.review_creation_date,ore.review_answer_timestamp)/60,0) AS hours_to_reply,
  CASE 
      WHEN ore.review_comment_message IS NOT NULL AND ore.review_comment_message != '' THEN 1 
      ELSE 0 
  END AS has_comment,
  CHAR_LENGTH(ore.review_comment_message) AS comment_count
FROM ods_olist_trd_order_reviews_df AS ore
JOIN ods_olist_trd_orders_df AS o
  ON ore.order_id=o.order_id
  AND ore.order_id IS NOT NULL
  AND ore.review_id IS NOT NULL
  AND o.customer_id IS NOT NULL
WHERE ore.review_score IS NOT NULL
  AND ore.review_score BETWEEN 1 AND 5
  AND ore.review_creation_date >= '${start_time}'
  AND ore.review_creation_date < '${end_time}';
