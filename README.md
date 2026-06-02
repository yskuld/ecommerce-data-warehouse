# ecommerce-data-warehouse
个人学习用，本项目用于展示数据仓库建模与ETL实践能力，持续更新中。

# Olist 电商数据仓库建模实践
> **一个从 0 到 1 的数据仓库项目，涵盖 ODS → DIM/DWD → DWS → ADS 分层设计与 ETL 实现，体现维度建模与业务理解能力。**


## 项目背景
基于巴西电商平台 Olist 的公开数据集（9 张业务表，包含订单、商品、支付、评价、顾客、商家等），模拟真实生产环境下的数据仓库建设流程。
**技术栈**：Hive SQL、维度建模（星型模型）、ORC + Snappy 压缩、分区表设计。


## 数据源说明
- 本项目使用 Olist 公开发布的 [Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)（Kaggle 数据集），共 9 张 CSV 表，涵盖 2016-2018 年的订单、商品、支付、评价、顾客、商家、地理位置等核心业务数据。
- 数据量级在约 10 万条订单、3 万条商品、4 万条支付记录。
- 数据集仅包含业务结果数据（交易、支付、评价），不包含用户行为日志和营销活动数据，因此当前模型聚焦交易域分析。
- 原表 `product_category_name_translation`（品类翻译表）无列名，已在 ODS 层手动定义列名后加载。


## 数据仓库架构
项目采用经典的 **ODS → DIM/DWD → DWS → ADS** 分层架构：
| 层级 | 说明 | 代表性表 |
|:---|:---|:---|
| **ODS** | 贴源层，完整保留原始数据，不做业务逻辑改动 | `ods_olist_trd_orders_df` 等 9 张表 |
| **DIM / DWD** | **DIM**：维度层，描述业务实体（商品、顾客、商家、日期），提供一致的维度属性。<br>**DWD**：明细事实层，基于星型模型构建，记录最原子的业务过程。DIM 与 DWD 在同一逻辑层内协同，共同构成星型模型的基础。 | `dim_olist_itm_product_1d`、`dim_date` 等 4 张维表；`dwd_olist_trd_ord_di`（累积快照）、`dwd_olist_trd_pay_di`（事务）、`dwd_olist_trd_rev_di`（事务）3 张事实表 |
| **DWS** | 汇总层，按维度实体轻度汇总，为 BI 看板和日报提供预聚合数据 | `dws_olist_trd_ord_by_date_1d`（日汇总）、`dws_olist_trd_ord_by_cate_1d`（品类汇总）已建，另有4张表建设中 |
| **ADS** | 应用层，面向业务角色组装宽表（待建设） | 规划中，对应运营、物流、支付、客服等角色看板 |


## 核心亮点
### 1. 严谨的维度建模
- 通过业务总线矩阵梳理出订单履约、支付、评价三个核心业务过程，构建星型模型。
- 事实表只存储维度外键和度量值，维度属性全部委托给 DIM 层，确保数据一致性与复用性。
- 所有 DWD 表均清晰声明粒度（如“订单-商品-商家”、“一笔支付流水”），遵循 Kimball 事实表设计原则。
### 2. 生产环境规范
- **表命名规范**：遵循 `分层_业务板块_数据域_业务过程_存储策略` 的通用标准，如 `dwd_olist_trd_ord_di`。
- **存储格式**：ORC 列式存储 + Snappy 压缩，兼顾查询性能与存储成本。
- **分区策略**：按 `dt`（数据日期）日分区，支持增量加载和历史回溯。
- **加载策略**：ODS 层区分全量快照（`_df`）与增量追加（`_di`）；DWD/DWS 层脚本同时支持全量初始化与每日增量调度，通过 `${start_time}`、`${end_time}`、`${dt}` 等调度参数灵活切换。
- **数据质量**：所有 ETL 脚本均包含主键非空、度量值非负、业务状态过滤等清洗逻辑。
### 3. 业务驱动指标设计
- 运用“规模-渗透率-深度”三维度框架，为核心业务角色（运营总监、物流经理、支付 PM、客服主管）设计汇总指标。
- 所有已规划和已建设的汇总表均基于**汇总维度矩阵**设计，指标与维度之间的对应关系清晰可查。


## 表清单（部分）
### ODS 层（贴源层）-此处展示全量快照表，配套有增量追加，后缀为`_di`
| 表名 | 数据域 | 说明 |
|:---|:---|:---|
| `ods_olist_trd_orders_df` | 交易域 | 订单全量表 |
| `ods_olist_trd_order_items_df` | 交易域 | 订单明细全量表 |
| `ods_olist_trd_order_payments_df` | 交易域 | 支付全量表 |
| `ods_olist_trd_order_reviews_df` | 交易域 | 评论全量表 |
| `ods_olist_crm_customers_df` | 客户域 | 顾客信息全量表 |
| `ods_olist_sel_sellers_df` | 商家域 | 商家信息全量表 |
| `ods_olist_itm_products_df` | 商品域 | 商品信息全量表 |
| `ods_olist_itm_category_translation_df` | 商品域 | 品类翻译全量表 |
| `ods_olist_pub_geolocation_df` | 公共域 | 地理位置全量表 |
### DIM 层（维度层）
| 表名 | 实体 | 说明 |
|:---|:---|:---|
| `dim_olist_itm_product_1d` | 商品 | 包含品类名称（英文）、重量尺寸等 |
| `dim_olist_crm_customer_1d` | 顾客 | 顾客城市、州等 |
| `dim_olist_sel_seller_1d` | 商家 | 商家城市、州等 |
| `dim_date` | 日期 | 日历表，含年、季度、月、周末标记 |
### DWD 层（明细事实层）
| 表名 | 类型 | 粒度 | 核心用途 |
|:---|:---|:---|:---|
| `dwd_olist_trd_ord_di` | 累积快照 | 订单-商品 | GMV、物流时效、商品件数 |
| `dwd_olist_trd_pay_di` | 事务 | 支付流水 | 支付金额、分期行为、支付摩擦 |
| `dwd_olist_trd_rev_di` | 事务 | 评价记录 | 评分、回复率、评价间隔 |
### DWS 层（汇总层，已建设）
| 表名 | 汇总维度 | 服务场景 |
|:---|:---|:---|
| `dws_olist_trd_ord_by_date_1d` | 时间（日） | 运营总监、物流经理每日看板 |
| `dws_olist_trd_ord_by_cate_1d` | 商品品类 | 品类运营与招商决策 |
### DWS 层待建设
- `dws_olist_trd_ord_by_cus_1d`：顾客地域汇总，支持用户画像与区域运营。
- `dws_olist_trd_ord_by_sel_1d`：商家地域汇总，支持商家运营与物流优化。
- `dws_olist_trd_pay_by_date_1d`：支付日汇总，支持支付健康度监控。
- `dws_olist_trd_rev_by_date_1d`：评价日汇总，支持服务质量分析。


## 文件结构
ecommerce-data-warehouse/
<br>├── README.md
<br>├── 01_etl_scripts/ 
<br>│ ├── 01_ods_tables.sql # ODS 层建表与全量加载
<br>│ ├── 02_dim_tables.sql # DIM 维度表建表与全量加载
<br>│ ├── 03_dwd_tables.sql # DWD 事实表建表与全量初始化
<br>│ ├── 04_dws_tables.sql # DWS 汇总表建表与增量加载
<br>│ ├── 05_ads_tables.sql # ODS 增量加载模板
<br>└── 02_daily/ # 生产环境每日增量调度示例
<br>│ ├── 06_ods_incremental_example.sql
<br>│ └── 07_dwd_incremental_example.sql
<br>└── 03_images/
<br>│ ├── bus_matrix.png # 业务总线矩阵（DWD层设计依据）
<br>│ └── summary_dim_matrix.png # 汇总维度矩阵（DWS层设计依据）


## 使用说明
1. **环境要求**：Hive 3.x 或 Spark SQL，支持 CTE 和窗口函数。
2. **执行顺序**：
   - **全量初始化**：按 `01 → 02 → 03 → 04` 顺序执行主脚本，完成各层建表及历史数据的首次全量加载。
   - **每日增量调度**：初始化完成后，ODS 层每日例行维护 `_df` 全量快照，并接入增量数据写入 `_di` 表；下游 DWD → DWS → ADS 各层，依次基于上一层前一日的增量结果，进行增量追加或覆盖更新。
3. **增量调度**：`daily/` 目录下的示例脚本展示了生产环境中每日增量加载的 SQL 逻辑，结合 Airflow 等调度工具即可投入日常使用。
4. **参数替换**：所有 `${dt}`、`${start_time}`、`${end_time}` 为调度参数，实际运行时由调度系统动态传入。


## 后续规划
- 完成顾客地域、商家地域、支付、评价的 DWS 汇总表。
- 建设 ADS 层，面向业务角色组装宽表，实现 DWS 原子汇总到业务看板的完整链路。
- 编写独立的质量检查脚本，每日自动运行，把控数据质量。
