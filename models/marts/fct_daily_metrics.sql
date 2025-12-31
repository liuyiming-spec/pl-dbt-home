-------------------------------过去第三十天 成本-------------------------------
-- 扣点
select 
    `date`,
    "ALL" as country,
    "commission_total_create_usd" as metric_name,
    sum(commission_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 退款B
select 
    `date`,
    "ALL" as country,
    "refund_b_total_create_usd" as metric_name,
    sum(refund_b_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 成本
select 
    `date`,
    "ALL" as country,
    "cost_total_create_usd" as metric_name,
    sum(cost_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
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
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all

-- 广告-CPC
select 
    `date`,
    "ALL" as country,
    "ads_cpc_cost_usd" as metric_name,
    sum(ads_cpc_cost_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all

-- 广告-Deal fee
select 
    `date`,
    "ALL" as country,
    "ads_deal_fee_usd" as metric_name,
    sum(ads_deal_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 广告-Coupon fee
select 
    `date`,
    "ALL" as country,
    "amz_channel_promote_coupon_usd" as metric_name,
    sum(amz_channel_promote_coupon_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 广告-Vine fee
select 
    `date`,
    "ALL" as country,
    "amz_vine_fee_usd" as metric_name,
    sum(amz_vine_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费
select 
    `date`,
    "ALL" as country,
    "freight_total_create_usd" as metric_name,
    sum(freight_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费-头程
select 
    `date`,
    "ALL" as country,
    "freight_headway_fee_create_usd" as metric_name,
    sum(freight_headway_fee_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费-尾程
select 
    `date`,
    "ALL" as country,
    "freight_fba_fees_create_usd" as metric_name,
    sum(freight_fba_fees_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费-月度仓储
select 
    `date`,
    "ALL" as country,
    "freight_inventory_fee_usd" as metric_name,
    sum(freight_inventory_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费-长期仓储
select 
    `date`,
    "ALL" as country,
    "freight_long_term_storage_fee_usd" as metric_name,
    sum(freight_long_term_storage_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费-其他FBA仓储
select 
    `date`,
    "ALL" as country,
    "freight_other_fba_storage_usd" as metric_name,
    sum(freight_other_fba_storage_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 仓库
select 
    `date`,
    "ALL" as country,
    "operating_jt_total_usd" as metric_name,
    sum(operating_jt_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运营人员
select 
    `date`,
    "ALL" as country,
    "operation_staff_total_usd" as metric_name,
    sum(operation_staff_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 支持人员
select 
    `date`,
    "ALL" as country,
    "support_staff_total_usd" as metric_name,
    sum(support_staff_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 研发费用
select 
    `date`,
    "ALL" as country,
    "dev_cost_total_usd" as metric_name,
    sum(dev_cost_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 其他费用
select 
    `date`,
    "ALL" as country,
    "other_fee_total_usd" as metric_name,
    sum(other_fee_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 税C
select 
    `date`,
    "ALL" as country,
    "tax_c_total_create_usd" as metric_name,
    sum(tax_c_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
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
-- 扣点
select 
    `date`,
    "ALL" as country,
    "commission_total_create_usd" as metric_name,
    sum(commission_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 退款B
select 
    `date`,
    "ALL" as country,
    "refund_b_total_create_usd" as metric_name,
    sum(refund_b_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type 
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all

-- 成本
select 
    `date`,
    "ALL" as country,
    "cost_total_create_usd" as metric_name,
    sum(cost_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all

-- 广告
select 
    `date`,
    "ALL" as country,
    "ads_total_usd" as metric_name,
    sum(ads_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type  
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all

-- 广告-CPC
select 
    `date`,
    "ALL" as country,
    "ads_cpc_cost_usd" as metric_name,
    sum(ads_cpc_cost_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all

-- 广告-Deal fee
select 
    `date`,
    "ALL" as country,
    "ads_deal_fee_usd" as metric_name,
    sum(ads_deal_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 广告-Coupon fee
select 
    `date`,
    "ALL" as country,
    "amz_channel_promote_coupon_usd" as metric_name,
    sum(amz_channel_promote_coupon_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 广告-Vine fee
select 
    `date`,
    "ALL" as country,
    "amz_vine_fee_usd" as metric_name,
    sum(amz_vine_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 运费
select 
    `date`,
    "ALL" as country,
    "freight_total_create_usd" as metric_name,
    sum(freight_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 运费-头程
select 
    `date`,
    "ALL" as country,
    "freight_headway_fee_create_usd" as metric_name,
    sum(freight_headway_fee_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
   `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 运费-尾程
select 
    `date`,
    "ALL" as country,
    "freight_fba_fees_create_usd" as metric_name,
    sum(freight_fba_fees_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运费-月度仓储
select 
    `date`,
    "ALL" as country,
    "freight_inventory_fee_usd" as metric_name,
    sum(freight_inventory_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 运费-长期仓储
select 
    `date`,
    "ALL" as country,
    "freight_long_term_storage_fee_usd" as metric_name,
    sum(freight_long_term_storage_fee_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 运费-其他FBA仓储
select 
    `date`,
    "ALL" as country,
    "freight_other_fba_storage_usd" as metric_name,
    sum(freight_other_fba_storage_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 仓库
select 
    `date`,
    "ALL" as country,
    "freight_other_fba_storage_usd" as metric_name,
    sum(freight_other_fba_storage_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    1 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by 
    1 

union all 

-- 运营人员
select 
    `date`,
    "ALL" as country,
    "operation_staff_total_usd" as metric_name,
    sum(operation_staff_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 支持人员
select 
    `date`,
    "ALL" as country,
    "support_staff_total_usd" as metric_name,
    sum(support_staff_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 研发费用
select 
    `date`,
    "ALL" as country,
    "dev_cost_total_usd" as metric_name,
    sum(dev_cost_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 其他费用
select 
    `date`,
    "ALL" as country,
    "other_fee_total_usd" as metric_name,
    sum(other_fee_total_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1 

union all 

-- 税C
select 
    `date`,
    "ALL" as country,
    "tax_c_total_create_usd" as metric_name,
    sum(tax_c_total_create_usd) as metric_value,
    sum(net_income_total_create_usd) as base_value,
    "cost" as field_type,
    2 as monitor_type
from 
    {{source("sales","ads_amazon_profit_model_tableau")}} 
where 
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
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
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
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
    `date` between DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) and CURRENT_DATE()
group by 
    1,2