-- 退款
select 
    `date`,
    "refund_b_sales_refund_create_usd" as metric_name,
    sum(refund_b_sales_refund_create_usd) as metric_value,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}}
group by 
    1 

union 

-- 广告
select 
    `date`,
    "ads_total_usd" as name,
    sum(ads_total_usd) as total,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}}
group by 
    1 