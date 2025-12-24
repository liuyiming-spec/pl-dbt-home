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
    {{ ref('metric_threshold_config') }}
)

-- 规则
