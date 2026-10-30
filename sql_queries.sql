-- Run each query for the current pipeline run by replacing :run_id with its UUID.
-- Financial metrics use only non-NULL values; SQL SUM/AVG naturally exclude NULL.

-- 1. Total order value
SELECT SUM(`Order Value`) AS total_order_value
FROM `cleaned_food_orders`
WHERE `pipeline_run_id` = :run_id AND `Order Value` IS NOT NULL;

-- 2. Average order value
SELECT AVG(`Order Value`) AS average_order_value
FROM `cleaned_food_orders`
WHERE `pipeline_run_id` = :run_id AND `Order Value` IS NOT NULL;

-- 3. Payment method with the most orders, excluding Unknown
SELECT `Payment Method`, COUNT(*) AS order_count
FROM `cleaned_food_orders`
WHERE `pipeline_run_id` = :run_id AND `Payment Method` <> 'Unknown'
GROUP BY `Payment Method`
ORDER BY order_count DESC, `Payment Method`
LIMIT 1;

-- 4. Order channel with the most orders
SELECT `Order Channel`, COUNT(*) AS order_count
FROM `cleaned_food_orders`
WHERE `pipeline_run_id` = :run_id
GROUP BY `Order Channel`
ORDER BY order_count DESC, `Order Channel`
LIMIT 1;

-- 5. Average valid delivery time
SELECT AVG(`Delivery_Time_Minutes`) AS average_delivery_time_minutes
FROM `cleaned_food_orders`
WHERE `pipeline_run_id` = :run_id AND `Delivery_Time_Minutes` IS NOT NULL;

-- 6. Total platform net revenue; rows missing any component have NULL revenue and are excluded
SELECT SUM(`Platform_Net_Revenue`) AS total_platform_net_revenue,
       COUNT(`Platform_Net_Revenue`) AS included_records
FROM `cleaned_food_orders`
WHERE `pipeline_run_id` = :run_id;
