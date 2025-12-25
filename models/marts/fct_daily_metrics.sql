-------------------------------过去第三十天 成本-------------------------------
-- 退款
select 
    `date`,
    "ALL" as country,
    "refund_b_total_create_usd" as metric_name,
    sum(refund_b_total_create_usd) as metric_value,
    sum(net_profit_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 广告
select 
    `date`,
    "ALL" as country,
    "ads_total_usd" as metric_name,
    sum(ads_total_usd) as metric_value,
    sum(net_profit_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all

-------------------------------过去第三十天 利润-------------------------------
-- 净利润
select 
    `date`,
    "ALL" as country,
    "net_profit_total_create_usd" as metric_name,
    sum(net_profit_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "profit" as field_type,
    1 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all

-------------------------------过去三十天内 成本-------------------------------
-- 退款
select 
    `date`,
    "ALL" as country,
    "refund_b_total_create_usd" as metric_name,
    sum(refund_b_total_create_usd) as metric_value,
    sum(net_profit_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all

-- 广告
select 
    `date`,
    "ALL" as country,
    "ads_total_usd" as metric_name,
    sum(ads_total_usd) as metric_value,
    sum(net_profit_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-------------------------------过去三十天内 利润-------------------------------
-- 净利润
select 
    `date`,
    "ALL" as country,
    "net_profit_total_create_usd" as metric_name,
    sum(net_profit_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "profit" as field_type,
    2 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-------------------------------过去三十天内每个站点 利润-------------------------------
-- 净利润
select 
    `date`,
    country,
    "net_profit_total_create_usd" as metric_name,
    sum(net_profit_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "profit" as field_type,
    3 as monitor_type  
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
group by 
    1,2