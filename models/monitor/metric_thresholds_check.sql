-- 增量更新
{{ 
  config(
    materialized = 'incremental',
    unique_key = ['alarm_date', 'metric_name'],
    incremental_strategy = 'merge'
  ) 
}}


-- 指标
with metric as (
  select 
    *
  from 
    {{ ref('fct_daily_metrics') }}
),

-- 阈值
threshold as (
  select 
    *
  from 
    {{ ref('metric_thresholds_config') }}
),

-- 字段映射
map as (
  select 
    *
  from 
    {{ ref('metric_cn_map') }}
),


-- 规则
base AS (
  SELECT
    a.date,
    a.monitor_type,
    a.field_type,
    a.metric_name,
    a.metric_value,
    a.base_value,
    SAFE_DIVIDE(a.metric_value, a.base_value) AS ratio,   -- 实际比率
    b.min_value,
    b.standard_value,
    b.max_value
  FROM metric a
  JOIN threshold b
    ON a.metric_name = b.metric_name
   AND a.monitor_type = b.monitor_type 
   AND EXTRACT(MONTH FROM a.date) BETWEEN start_month AND end_month
),

scored AS (
  SELECT
    *,
    -- 退款状态
    CASE
      WHEN monitor_type IN (1, 3) THEN '退款周期已完成'
      WHEN monitor_type = 2 THEN '退款周期未完成'
      ELSE NULL
    END AS refund_status 
  FROM base
),

alarm AS (
  SELECT
    date,
    monitor_type,
    field_type,
    metric_name,
    ratio,
    min_value,
    standard_value,
    max_value,
    refund_status,

    -- 报警类型
    CASE
      WHEN monitor_type = 1 AND field_type = 'cost' AND ratio <= min_value THEN '核实'
      WHEN monitor_type IN (1, 2) AND field_type = 'cost' AND ratio >= max_value THEN '预警'

      WHEN monitor_type IN (1, 2) AND field_type = 'profit' AND ratio <= min_value THEN '预警'
      WHEN monitor_type = 1 AND field_type = 'profit' AND ratio >= max_value THEN '核实'

      WHEN monitor_type = 3 AND field_type = 'profit' AND ratio <= min_value THEN '预警'
    END AS alarm_type,

    -- 预警值/核实值
    CASE
      WHEN monitor_type = 1 AND field_type = 'cost' AND ratio <= min_value THEN min_value
      WHEN monitor_type IN (1, 2) AND field_type = 'cost' AND ratio >= max_value THEN max_value

      WHEN monitor_type IN (1, 2) AND field_type = 'profit' AND ratio <= min_value THEN min_value
      WHEN monitor_type = 1 AND field_type = 'profit' AND ratio >= max_value THEN max_value

      WHEN monitor_type = 3 AND field_type = 'profit' AND ratio <= min_value THEN min_value
    END AS alarm_value
  FROM scored
)

SELECT
  a.date,
  a.refund_status,
  a.metric_name,
  b.metric_cn_name,
  a.alarm_type,
  a.alarm_value,
  a.standard_value,
  ROUND(a.ratio, 4) AS actual_value,  --实际值
  ROUND(a.ratio - a.standard_value, 4) AS standard_diff_value, 
  ROUND(a.ratio - a.alarm_value, 4) AS alarm_diff_value,  
  CURRENT_DATE() AS alarm_date
FROM alarm a 
left join
    map b 
on 
    a.metric_name = b.metric_name 
where alarm_type is not null
    