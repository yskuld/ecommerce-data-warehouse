-- ==========================================
-- ODS层：增量加载示例（生产环境每日调度）
-- 说明：
--   全量初始化脚本（../01_ods_int.sql）已通过 CTAS 创建 _df 全量表并加载全部历史数据。
--   此脚本用于接入真实业务流后，每日从源系统加载增量数据到 ODS _di 表。
--   源系统假设为已经落地的业务数据库（如 MySQL），或通过 Kafka 接入的实时流。
--   调度工具（Airflow）每日凌晨触发，传入 ${dt}、${start_time}、${end_time} 参数。
--
-- 建表策略：
--   ODS _di 表在首次执行时通过 `CREATE TABLE IF NOT EXISTS ... LIKE` 创建，
--   结构完全复制对应的 _df 全量表，确保后续 DWD 层引用一致。
--   （若已在其他初始化脚本中创建，该语句会自动跳过，不会重复执行。）
--
-- 核心变化（与全量初始化对比）：
--   - 目标表：使用 _di 增量表
--   - 写入方式：INSERT INTO 追加当天分区（而非 INSERT OVERWRITE）
--   - 数据源：从源系统读取，使用时间戳过滤昨日新增
-- ==========================================

-- 示例：交易域订单表 增量加载
-- 1. 确保 _di 表已存在（结构与 _df 表相同）
CREATE TABLE IF NOT EXISTS ods_olist_trd_orders_di LIKE ods_olist_trd_orders_df;

-- 2. 加载昨日增量数据
INSERT INTO TABLE ods_olist_trd_orders_di
PARTITION (dt = '${dt}')
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM source_orders_table  -- 假设源系统已接入，替换为实际源表名
WHERE order_purchase_timestamp >= '${start_time}'
  AND order_purchase_timestamp < '${end_time}';


-- 其他 ODS 增量表同理，仅替换表名、字段和源表
