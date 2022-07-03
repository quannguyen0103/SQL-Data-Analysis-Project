
-- calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
  format_date( '%Y%m',parse_date('%Y%m%d',date)) as month,
  sum(totals.visits) as visit,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue)/power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group by month
order by month;

-- Bounce rate per traffic source in July 2017
#standardSQL
select
  trafficSource.source as source,
  sum(totals.visits) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  sum(totals.bounces)/sum(totals.visits)*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc;

-- Revenue by traffic source by week, by month in June 2017
#standardSQL
select
   case when date like '201706%' then 'month' end as time_type,
   format_date('%Y%m',parse_date('%Y%m%d',date)) as time,
   trafficSource.source as source,
   sum(totals.totalTransactionRevenue)/power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by time,time_type,source
union all
select 
   case when date like '201706%' then 'week' end as time_type,
   format_date('%Y%W',parse_date('%Y%m%d',date)) as time,
   trafficSource.source as source,
   sum(totals.totalTransactionRevenue)/power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by time,time_type,source
order by revenue desc;

-- Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with pageviews_purchase as
(select
  format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
  sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170731'
  and totals.transactions >=1
group by month),
pageviews_non_purchase as
(select
  format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
  sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170731'
  and totals.transactions is null
group by month)
select 
  pp.*,
  pn.avg_pageviews_non_purchase
from pageviews_purchase as pp
join pageviews_non_purchase as pn
  on pp.month = pn.month
order by month;

-- Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >= 1
group by month;

-- Average amount of money spent per session
#standardSQL
select
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  format("%'f",sum(totals.totalTransactionRevenue)/count(totals.visits)) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >= 1
group by month;

-- Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with purchasers as
(select
  fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) as hits,
  UNNEST (hits.product) as product
where product.v2ProductName = "YouTube Men's Vintage Henley"
and product.productRevenue is not null)
select 
  product.v2ProductName as other_purchased_products,
  sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) as hits,
  UNNEST (hits.product) as product
where fullVisitorId in (select fullVisitorId from purchasers)
and product.productRevenue is not null
and product.v2ProductName != "YouTube Men's Vintage Henley"
group by other_purchased_products
order by quantity desc;

-- Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with product_view as
(select
  format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  count(hits.eCommerceAction.action_type) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) as hits
where _table_suffix between '20170101' and '20170331'
  and hits.eCommerceAction.action_type = '2'
group by month),
add_to_cart as
(select
  format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  count(hits.eCommerceAction.action_type) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) as hits
where _table_suffix between '20170101' and '20170331'
  and hits.eCommerceAction.action_type = '3'
group by month),
purchase as
(select
  format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  count(hits.eCommerceAction.action_type) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) as hits
where _table_suffix between '20170101' and '20170331'
  and hits.eCommerceAction.action_type = '6'
group by month)
select
  pv.*,
  ac.num_addtocart,
  p.num_purchase,
  round((num_addtocart/num_product_view)*100,2) as add_to_cart_rate,
  round((num_purchase/num_product_view)*100,2) as purchase_rate
from product_view as pv
  join add_to_cart as ac
  on pv.month = ac.month
  join purchase as p
  on ac.month = p.month
order by month;
