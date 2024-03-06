
--Q1 Calculate total visit, pageview, transaction for Jan, Feb and March 2017

SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY 1
ORDER BY 1;

--Q2 Bounce rate per traffic source in July 2017

SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;

  
--Q3 Revenue by traffic source by week, by month in June 2017

WITH combine AS (
    SELECT 
        'Month' AS time_type, 
        FORMAT_DATE('%Y%m',(parse_date('%Y%m%d',date))) AS time,
        trafficSource.source AS source, 
        ROUND(SUM(product.productRevenue)/ 1000000,4) AS revenue 
    FROM 
        `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
        UNNEST (hits) hits,
        UNNEST (hits.product) product
    WHERE 
        product.productRevenue IS NOT NULL
    GROUP BY 
        time_type,source,time

    UNION ALL

    SELECT 
        'Week' AS time_type, 
        FORMAT_DATE('%Y%W',(parse_date('%Y%m%d',date))) AS time,
        trafficSource.source AS source, 
        ROUND(SUM(product.productRevenue)/ 1000000,4) AS revenue 
    FROM 
        `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
        UNNEST (hits) hits,
        UNNEST (hits.product) product
    WHERE 
        product.productRevenue IS NOT NULL
    GROUP BY 
    time_type,source,time
)

SELECT * 
FROM combine 
ORDER BY 
time_type, time, source,revenue DESC;

-- Q4 Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

with 
purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  and product.productRevenue is not null
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;

-- Q5 Average number of transactions per user that made a purchase in July 2017

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
where  totals.transactions>=1
and totals.totalTransactionRevenue is not null
and product.productRevenue is not null
group by month;

-- Q6 Average amount of money spent per session. Only include purchaser data in July 2017

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
group by month;

-- Q7 Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017

WITH main_table AS (
    SELECT  
        fullVisitorId AS user_id,
        product.v2ProductName AS product_name
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
)

SELECT
    product_name AS other_purchased_products,
    COUNT(*) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
WHERE fullVisitorId IN (
        SELECT user_id
        FROM main_table
    )
    AND product.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity DESC;

-- Q8 Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;
