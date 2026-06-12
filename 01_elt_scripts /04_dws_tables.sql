-- ==========================================
-- DWS层：数据汇总层构建脚本
-- 设计依据：Kimball 维度建模理论 + 汇总维度矩阵
-- 包含汇总表：
--   - dws_olist_trd_ord_by_date_1d（订单履约日汇总）
--   - dws_olist_trd_ord_by_cate_1d（订单履约品类汇总）
--   - dws_olist_trd_ord_by_cus_1d（订单履约顾客地域汇总）-- 待建设
--   - dws_olist_trd_ord_by_sel_1d（订单履约商家地域汇总）-- 待建设
--   - dws_olist_trd_pay_by_date_1d（订单支付日汇总）-- 待建设
--   - dws_olist_trd_rev_by_date_1d（订单评价日汇总）-- 待建设
--
-- 加载策略：
--   1. 全量初始化：
--      使用 INSERT OVERWRITE PARTITION (dt) + 时间戳过滤参数，
--      从 DWD 明细表一次性加载全部历史数据，完成 DWS 层的初始化。
--   2. 每日增量调度（生产环境切换，见下方代码备注）：
--      接入真实业务流后，DWD 层每日产生新分区，
--      DWS 层改为从 DWD 的 dt='${dt}' 分区读取昨日增量明细，
--      通过 INSERT OVERWRITE PARTITION (dt='${dt}') 覆盖写入昨日分区。
--
-- 设计原则：
--   - 维度实体驱动：一张汇总表只围绕一个核心维度实体构建，如时间维度、品类维度。
--   - 指标完整性：每个汇总维度下，以业务性质为纲，充分考虑规模指标（GMV、订单数）、
渗透率指标（新用户占比、超时率）与深度指标（平均订单金额、客单价、平均承运时长）的全面性。
--   - 避免重复计算：对于订单级度量（如承运时长、超时标记），
先在子查询中去重至订单粒度，再按维度聚合，确保数据准确。
--   - 与 ADS 层分工：DWS 层只提供原子化的维度汇总，
面向业务角色的看板宽表留给 ADS 层拼接。
--
-- 技术栈：Hive SQL / 维度建模 / 星型模型
-- 存储格式：ORC 列式存储 + Snappy 压缩
-- 分区策略：按 dt（数据日期）日分区


-- 待建设说明：所有已规划待建设表的结构和聚合逻辑已在汇总维度矩阵中明确，可随时落地。
-- ==========================================



-- ==========================================
-- DWS层：订单履约日汇总表
-- 表名：dws_olist_trd_ord_by_date_1d
-- 粒度：每天一行
-- 来源：dwd_olist_trd_ord_di
-- 说明：每一行订单-商品代表一件商品，金额独立。
--       订单级度量（承运、耗时、超时）需先去重再聚合。
--       同时保留平均订单金额(AOV)和客单价(ARPU)。
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dws_olist_trd_ord_by_date_1d (
    date_id               STRING        COMMENT '日期',
    daily_gmv             DECIMAL(10,2) COMMENT '日GMV',
    daily_order_count     INT           COMMENT '日订单数',
    daily_customer_count  INT           COMMENT '日下单客户数',
    avg_order_amount      DECIMAL(10,2) COMMENT '平均订单金额(AOV)',
    avg_customer_amount   DECIMAL(10,2) COMMENT '客单价(ARPU)',
    avg_item_count        DECIMAL(10,2) COMMENT '平均商品件数',
    new_customer_rate     DECIMAL(10,4) COMMENT '新用户占比',
    avg_deliver_duration  DECIMAL(10,2) COMMENT '平均承运时长(天)',
    avg_total_duration    DECIMAL(10,2) COMMENT '平均全流程耗时(天)',
    overtime_rate         DECIMAL(10,4) COMMENT '超时率'
)
COMMENT '订单履约日汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dws_olist_trd_ord_by_date_1d
PARTITION (dt = '${dt}')
WITH
-- 基础数据集：商品行 + 动态标记
base AS (
    SELECT
        o.date_id,
        o.order_id,
        o.customer_id,
        o.total_amount,
        o.deliver_duration,
        o.total_duration,
        -- 超时标记：实际签收 > 预计送达
        CASE
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
            THEN 1 ELSE 0
        END AS is_overtime,
        -- 新用户标记：首次下单日期 = 当前订单日期
        CASE
            WHEN MIN(DATE(o.order_purchase_timestamp)) OVER (PARTITION BY o.customer_id)
                 = DATE(o.order_purchase_timestamp)
            THEN 1 ELSE 0
        END AS is_new_customer
    FROM dwd_olist_trd_ord_di o
    -- 全量初始化：使用时间戳范围过滤，覆盖全部历史数据
    WHERE o.order_purchase_timestamp >= '${start_time}'
      AND o.order_purchase_timestamp < '${end_time}'
    -- 每日加载时替换为：
    -- WHERE o.dt = '${dt}'

),

-- 直接聚合的指标（无需订单级去重）
daily_direct AS (
    SELECT
        date_id,
        SUM(total_amount) AS daily_gmv,
        COUNT(DISTINCT order_id)                        AS daily_order_count,
        COUNT(DISTINCT customer_id)                     AS daily_customer_count,
        -- AOV
        SUM(total_amount) / COUNT(DISTINCT order_id)    AS avg_order_amount,
        -- ARPU
        SUM(total_amount) / COUNT(DISTINCT customer_id) AS avg_customer_amount,

        COUNT(*) / COUNT(DISTINCT order_id)             AS avg_item_count,

        COUNT(DISTINCT CASE WHEN is_new_customer = 1 THEN customer_id END)
            / COUNT(DISTINCT customer_id)               AS new_customer_rate
    FROM base
    GROUP BY date_id
),

-- 订单级去重：用于承运时长、全流程耗时、超时率
order_level AS (
    SELECT
        date_id,
        order_id,
        MAX(deliver_duration) AS order_deliver_duration,
        MAX(total_duration)   AS order_total_duration,
        MAX(is_overtime)      AS is_overtime
    FROM base
    GROUP BY date_id, order_id
),
daily_duration_overtime AS (
    SELECT
        date_id,
        AVG(order_deliver_duration) AS avg_deliver_duration,
        AVG(order_total_duration)   AS avg_total_duration,
        AVG(is_overtime)            AS overtime_rate
    FROM order_level
    GROUP BY date_id
)

-- 最终合并
SELECT
    d.date_id,
    d.daily_gmv,
    d.daily_order_count,
    d.daily_customer_count,
    d.avg_order_amount,
    d.avg_customer_amount,
    d.avg_item_count,
    d.new_customer_rate,
    o.avg_deliver_duration,
    o.avg_total_duration,
    o.overtime_rate
FROM daily_direct d
LEFT JOIN daily_duration_overtime o ON d.date_id = o.date_id
;

-- ==========================================
-- DWS层：订单履约品类日汇总表
-- 表名：dws_olist_trd_ord_by_cate_1d
-- 粒度：每个品类每天一行
-- 来源：dwd_olist_trd_ord_di + dim_olist_itm_product_1d
-- 说明：每一行订单-商品代表一件商品，price/freight_value均为该件独立金额。
--       品类GMV、订单数、客单价、平均商品件数等可直接基于商品行计算；
--       承运时长、全流程耗时、超时率等订单级字段需按(品类,订单)去重。
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dws_olist_trd_ord_by_cate_1d (
    cate_name                   STRING        COMMENT '品类名称（英文）',
    cate_gmv                    DECIMAL(10,2) COMMENT '品类GMV',
    cate_order_count            INT           COMMENT '品类订单数',
    cate_customer_count         INT           COMMENT '品类下单客户数',
    cate_avg_order_amount       DECIMAL(10,2) COMMENT '品类平均订单金额',
    cate_avg_customer_amount    DECIMAL(10,2) COMMENT '品类客单价（ARPU）',
    cate_avg_item_count         DECIMAL(10,2) COMMENT '品类平均商品件数（每单购买件数）',
    cate_new_customer_rate      DECIMAL(10,4) COMMENT '品类新用户占比',
    cate_avg_deliver_duration   DECIMAL(10,2) COMMENT '品类平均承运时间（天）',
    cate_avg_total_duration     DECIMAL(10,2) COMMENT '品类平均全流程耗时（天）',
    cate_overtime_rate          DECIMAL(10,4) COMMENT '品类超时率'
)
COMMENT '订单履约品类日汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dws_olist_trd_ord_by_cate_1d
PARTITION (dt = '${dt}')
WITH
-- 基础数据集：商品行 + 品类名称 + 动态计算标记
base AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.total_amount,
        o.deliver_duration,
        o.total_duration,
        -- 超时标记：实际签收 > 预计送达
        CASE
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
            THEN 1 ELSE 0
        END AS is_overtime,
        -- 新用户标记：首次下单日期 = 当前订单日期
        CASE
            WHEN MIN(DATE(o.order_purchase_timestamp)) OVER (PARTITION BY o.customer_id)
                 = DATE(o.order_purchase_timestamp)
            THEN 1 ELSE 0
        END AS is_new_customer,
        p.product_category_name_en AS cate_name
    FROM dwd_olist_trd_ord_di o
    JOIN dim_olist_itm_product_1d p ON o.product_id = p.product_id
    -- 全量初始化：使用时间戳范围过滤，覆盖全部历史数据
    WHERE o.order_purchase_timestamp >= '${start_time}'
      AND o.order_purchase_timestamp < '${end_time}'
    -- 每日加载时替换为：
    -- WHERE o.dt = '${dt}'

),

-- 直接按品类聚合的指标
cate_direct AS (
    SELECT
        cate_name,
        SUM(total_amount)                              AS cate_gmv,
        COUNT(DISTINCT order_id)                       AS cate_order_count,
        COUNT(DISTINCT customer_id)                    AS cate_customer_count,
        SUM(total_amount) / COUNT(DISTINCT order_id)   AS cate_avg_order_amount,
        SUM(total_amount) / COUNT(DISTINCT customer_id) AS cate_avg_customer_amount,

        COUNT(*) / COUNT(DISTINCT order_id)            AS cate_avg_item_count,
        COUNT(DISTINCT CASE WHEN is_new_customer = 1 THEN customer_id END)
            / COUNT(DISTINCT customer_id)              AS cate_new_customer_rate
    FROM base
    GROUP BY cate_name
),

--注意去重：用于承运时长、全流程耗时、超时率
cate_duration AS (
    SELECT
        cate_name,
        order_id,
        MAX(deliver_duration) AS order_deliver_duration,
        MAX(total_duration)   AS order_total_duration
    FROM base
    GROUP BY cate_name, order_id
),
cate_avg_duration AS (
    SELECT
        cate_name,
        AVG(order_deliver_duration) AS cate_avg_deliver_duration,
        AVG(order_total_duration)   AS cate_avg_total_duration
    FROM cate_duration
    GROUP BY cate_name
),

cate_overtime AS (
    SELECT
        cate_name,
        order_id,
        MAX(is_overtime) AS max_overtime
    FROM base
    GROUP BY cate_name, order_id
),
cate_avg_overtime AS (
    SELECT
        cate_name,
        AVG(max_overtime) AS cate_overtime_rate
    FROM cate_overtime
    GROUP BY cate_name
)

-- 最终合并
SELECT
    d.cate_name,
    d.cate_gmv,
    d.cate_order_count,
    d.cate_customer_count,
    d.cate_avg_order_amount,
    d.cate_avg_customer_amount,
    d.cate_avg_item_count,
    d.cate_new_customer_rate,
    du.cate_avg_deliver_duration,
    du.cate_avg_total_duration,
    o.cate_overtime_rate
FROM cate_direct d
LEFT JOIN cate_avg_duration du ON d.cate_name = du.cate_name
LEFT JOIN cate_avg_overtime o  ON d.cate_name = o.cate_name
;

-- ==========================================
-- DWS层：订单支付日汇总表
-- 表名：dws_olist_trd_pay_by_date_1d
-- 粒度：每天一行
-- 来源：dwd_olist_trd_pay_di
-- 说明：支付金额、支付笔数、分期占比、平均期数可直接基于支付流水行聚合；
--       支付流水笔数（payment_attempts）为订单级度量，需先按订单去重再平均。
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dws_olist_trd_pay_by_date_1d (
    date_id                 STRING        COMMENT '日期',
    daily_payment_value     DECIMAL(10,2) COMMENT '日支付总金额',
    daily_payment_count     INT           COMMENT '日支付流水笔数',
    installment_rate        DECIMAL(10,4) COMMENT '分期占比（按笔数）',
    installment_gmv_rate    DECIMAL(10,4) COMMENT '分期GMV占比（按金额）',
    avg_installments        DECIMAL(10,2) COMMENT '分期用户平均期数',
    avg_payment_attempts    DECIMAL(10,2) COMMENT '平均每订单支付流水笔数'
)
COMMENT '订单支付日汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dws_olist_trd_pay_by_date_1d
PARTITION (dt = '${dt}')
WITH
-- 基础数据集：支付流水
base AS (
    SELECT
        p.date_id,
        p.order_id,
        p.payment_value,
        p.is_installment,
        p.payment_installments,
        p.payment_attempts
    FROM dwd_olist_trd_pay_di p
    JOIN dwd_olist_trd_ord_di o ON p.order_id = o.order_id
    -- 全量初始化：使用订单的下单时间戳范围过滤，覆盖全部历史数据
    WHERE o.order_purchase_timestamp >= '${start_time}'
      AND o.order_purchase_timestamp < '${end_time}'
    -- 每日加载时替换为：
    -- WHERE p.dt = '${dt}'
),

-- 直接聚合的指标（支付金额、笔数、分期占比、平均期数）
pay_direct AS (
    SELECT
        date_id,
        SUM(payment_value) AS daily_payment_value,
        COUNT(*) AS daily_payment_count,
        SUM(is_installment) / COUNT(*) AS installment_rate,
        SUM(CASE WHEN is_installment = 1 THEN payment_value ELSE 0 END) / SUM(payment_value) AS installment_gmv_rate,
        AVG(CASE WHEN is_installment = 1 THEN payment_installments END) AS avg_installments
    FROM base
    GROUP BY date_id
),

-- 订单级去重：用于支付流水笔数
order_attempts AS (
    SELECT
        date_id,
        order_id,
        MAX(payment_attempts) AS order_payment_attempts
    FROM base
    GROUP BY date_id, order_id
),
avg_attempts AS (
    SELECT
        date_id,
        AVG(order_payment_attempts) AS avg_payment_attempts
    FROM order_attempts
    GROUP BY date_id
)

-- 最终合并
SELECT
    d.date_id,
    d.daily_payment_value,
    d.daily_payment_count,
    d.installment_rate,
    d.installment_gmv_rate,
    d.avg_installments,
    a.avg_payment_attempts
FROM pay_direct d
LEFT JOIN avg_attempts a ON d.date_id = a.date_id
;

-- ==========================================
-- DWS层：订单评价日汇总表
-- 表名：dws_olist_trd_rev_by_date_1d
-- 粒度：每天一行
-- 来源：dwd_olist_trd_rev_di
-- 说明：评价事实表粒度为一条评价记录，所有指标可直接基于评价行聚合，无需去重。
-- ==========================================

-- 1. 建表
CREATE TABLE IF NOT EXISTS dws_olist_trd_rev_by_date_1d (
    date_id             STRING        COMMENT '日期',
    daily_review_count  INT           COMMENT '日评价数',
    avg_review_score    DECIMAL(10,2) COMMENT '平均评分',
    good_review_rate    DECIMAL(10,4) COMMENT '好评率',
    reply_rate          DECIMAL(10,4) COMMENT '回复率',
    avg_hours_to_reply  DECIMAL(10,2) COMMENT '平均回复时长（小时）',
    has_comment_rate    DECIMAL(10,4) COMMENT '有内容评价占比'
)
COMMENT '订单评价日汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 2. 加载数据
INSERT OVERWRITE TABLE dws_olist_trd_rev_by_date_1d
PARTITION (dt = '${dt}')
WITH
-- 基础数据集：评价记录 + 关联订单表获取下单时间戳（用于全量初始化时间范围过滤）
base AS (
    SELECT
        r.date_id,
        r.review_score,
        r.is_good_review,
        r.is_reply,
        r.hours_to_reply,
        r.has_comment
    FROM dwd_olist_trd_rev_di r
    JOIN dwd_olist_trd_ord_di o ON r.order_id = o.order_id
    -- 全量初始化：使用订单的下单时间戳范围过滤
    WHERE o.order_purchase_timestamp >= '${start_time}'
      AND o.order_purchase_timestamp < '${end_time}'
    -- 每日加载时替换为：
    -- WHERE r.dt = '${dt}'
)

-- 聚合
SELECT
    date_id,
    COUNT(1) AS daily_review_count,
    AVG(review_score) AS avg_review_score,
    SUM(is_good_review) / COUNT(1) AS good_review_rate,
    SUM(is_reply) / COUNT(1) AS reply_rate,
    AVG(hours_to_reply) AS avg_hours_to_reply,
    SUM(has_comment) / COUNT(1) AS has_comment_rate
FROM base
GROUP BY date_id
;
