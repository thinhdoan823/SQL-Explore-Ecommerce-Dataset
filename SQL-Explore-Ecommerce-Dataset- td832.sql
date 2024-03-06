
--Q1 Calculate total visit, pageview, transaction for Jan, Feb and March 2017

SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    SUM(totals.visits) AS visits,
    SUM(totals.pageviews) AS pageviews,
    SUM(totals.transactions) AS transactions,
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE 
  _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY 1
ORDER BY 1;

--Q2 Bounce rate per traffic source in July 2017

SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
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

WITH 
purchaser_data AS (
  SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
      (SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid)) AS avg_pageviews_purchase
  FROM 
      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits,
      UNNEST(product) AS product
  WHERE 
      _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions >= 1
      AND product.productRevenue IS NOT NULL
  GROUP BY month
),

non_purchaser_data AS (
  SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
      SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid) AS avg_pageviews_non_purchase
  FROM 
      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits,
      UNNEST(product) AS product
  WHERE 
      _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions IS NULL
      AND product.productRevenue IS NULL
  GROUP BY month
)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data pd
LEFT JOIN non_purchaser_data USING(month)
ORDER BY pd.month;

-- Q5 Average number of transactions per user that made a purchase in July 2017

SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    SUM(totals.transactions)/COUNT(DISTINCT fullvisitorid) AS Avg_total_transactions_per_user
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST(product) AS product
WHERE  
    totals.transactions >= 1
    AND totals.totalTransactionRevenue IS NOT NULL
    AND product.productRevenue IS NOT NULL
GROUP BY month;

-- Q6 Average amount of money spent per session. Only include purchaser data in July 2017

SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    ((SUM(product.productRevenue)/SUM(totals.visits))/POWER(10,6)) AS avg_revenue_by_user_per_visit
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST(product) AS product
WHERE 
    product.productRevenue IS NOT NULL
GROUP BY month;

-- Q7 Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017

WITH main_table AS (
    SELECT  
        fullVisitorId AS user_id,
        product.v2ProductName AS product_name
    FROM 
        `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST (hits) AS hits,
        UNNEST (hits.product) AS product
    WHERE 
        product.v2ProductName = "YouTube Men's Vintage Henley"
)

SELECT
    product_name AS other_purchased_products,
    COUNT(*) AS quantity
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
WHERE 
    fullVisitorId IN (
        SELECT user_id
        FROM main_table
    )
    AND product.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity DESC;

-- Q8 Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017

WITH product_data AS (
  SELECT
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) AS num_product_view,
      COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) AS num_add_to_cart,
      COUNT(CASE WHEN eCommerceAction.action_type = '6' AND product.productRevenue IS NOT NULL THEN product.v2ProductName END) AS num_purchase
  FROM 
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST(hits) AS hits,
      UNNEST(hits.product) AS product
  WHERE 
      _table_suffix BETWEEN '20170101' AND '20170331'
      AND eCommerceAction.action_type IN ('2','3','6')
  GROUP BY month
  ORDER BY month
)

SELECT
    *,
    ROUND(num_add_to_cart/num_product_view * 100, 2) AS add_to_cart_rate,
    ROUND(num_purchase/num_product_view * 100, 2) AS purchase_rate
FROM product_data;
