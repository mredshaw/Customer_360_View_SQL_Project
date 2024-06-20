-- Customer 360
-- Group 13

-- Code from class however we added a few columns that we needed for later tables including:
-- sk_customer, conversion_id, order number, conversion channel, and year week
-- We also created a new value called next conversion week, this uses a lead statement similar to next conversion date but uses week instead
-- An additional join was added to be able to include year week and join all values this table produces with possible values from the date_dimension using an inner join
WITH conversions_with_customer_id AS (
SELECT cd.customer_id,
       cd.first_name,
       cd.last_name,
       cd.sk_customer,
       ROW_NUMBER() over (PARTITION BY cd.customer_id ORDER BY cs.conversion_date) AS conversion_number,
       cs.conversion_date,
       cs.conversion_id,
       LEAD(cs.conversion_date) OVER (PARTITION BY cd.customer_id ORDER BY cs.conversion_date) AS next_conversion_date,
       LEAD(dd.year_week) OVER (PARTITION BY cd.customer_id ORDER BY cs.conversion_date) AS next_conversion_week,
       cs.order_number,
       cs.conversion_type,
       cs.conversion_channel,
       dd.year_week
FROM fact_tables.conversions AS cs
INNER JOIN dimensions.customer_dimension AS cd
    ON cs.fk_customer = cd.sk_customer
INNER JOIN dimensions.date_dimension AS dd
    ON cs.fk_conversion_date = dd.sk_date
),

-- This is another class query but we added sk_customer, and discount
orders_with_customer_id AS (
SELECT cd.customer_id,
       cd.first_name,
       cd.last_name,
       ROW_NUMBER() over (PARTITION BY cd.customer_id ORDER BY o.order_date) order_recurrence,
       cd.sk_customer,
       o.order_date,
       LEAD(o.order_date) OVER (PARTITION BY cd.customer_id ORDER BY o.order_date) next_order_date,
       o.order_number,
       pd.product_name,
       o.price_paid,
       o.discount
FROM fact_tables.orders AS o
INNER JOIN dimensions.customer_dimension AS cd
  ON o.fk_customer = cd.sk_customer
INNER JOIN dimensions.product_dimension  AS pd
  ON o.fk_product = pd.sk_product
),

-- This is the last class table and nothing was changed.
conversions_with_first_orders AS (
SELECT cs.*,
       o.order_date,
       o.product_name,
       o.sk_customer
FROM conversions_with_customer_id AS cs
LEFT JOIN orders_with_customer_id AS o
  ON cs.order_number = o.order_number
),

-- by setting the order recurrence to one we were able to retrieve all of the first order values needed for the table
first_order_values AS (
SELECT ci.customer_id,
       ci.sk_customer,
       cs.order_number AS first_order_number,
       cs.order_date AS first_order_date,
       ci.year_week AS first_order_week,
       cs.product_name AS first_order_product,
       cs.price_paid AS first_order_price_paid,
       cs.discount AS first_order_discount,
       (cs.price_paid + cs.discount) AS first_order_unit_price
FROM orders_with_customer_id AS cs
LEFT JOIN conversions_with_customer_id AS ci
    ON cs.order_number = ci.order_number
WHERE cs.order_recurrence= 1
),

-- This was to link all the values from the orders table to to customer_id and sk_customer
-- We did this through the use of a left join so now we could look at orders using customer_id
orders_customers AS (
SELECT o.*,
       cd.customer_id,
       cd.sk_customer
FROM fact_tables.orders AS o
LEFT JOIN dimensions.customer_dimension AS cd
    ON o.fk_customer = cd.sk_customer
ORDER BY cd.customer_id
),

-- This query was to perform a similar function to before being able to link dates and the customer dimension
dates_customers AS (
SELECT dd.*,
       cd.*
FROM dimensions.customer_dimension AS cd
CROSS JOIN dimensions.date_dimension AS dd
ORDER BY cd.customer_id, dd.date
),

-- Combining the two queries above allows us to retrieve all columns we need by joining on customer_id and date
orders_dates_customers AS (
SELECT con.conversion_id,
       con.fk_customer,
       oc.order_number,
       oc.fk_order_date,
       oc.order_id,
       oc.fk_customer,
       oc.order_date,
       oc.order_item_id,
       oc.fk_product,
       oc.unit_price,
       oc.price_paid,
       oc.discount,
       dc.customer_id,
       dc.sk_date,
       dc.week,
       dc.sk_customer,
       dc.date,
       dc.year_week
FROM dates_customers AS dc
LEFT JOIN orders_customers AS oc
    ON dc.customer_id = oc.customer_id AND dc.sk_date = oc.fk_order_date
LEFT JOIN fact_tables.conversions AS con
    ON con.fk_conversion_date = dc.sk_date
ORDER BY dc.customer_id, dc.sk_date
),

-- In the next two queries we needed to find the total revenue, discount, and cumulative revenue
-- Since we needed aggregate, therefore needing to group on specific parameters, we had to split cumulative revenue, and cumulative order lifetime
revenue AS (
SELECT  odc.customer_id,
        odc.year_week,
        SUM(odc.price_paid) AS revenue,
        SUM(odc.discount) AS total_discount,
        SUM(SUM(CASE WHEN odc.price_paid IS NOT NULL THEN odc.price_paid ELSE 0 END))
            OVER (PARTITION BY odc.customer_id ORDER BY odc.year_week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
FROM orders_dates_customers as odc
GROUP BY odc.customer_id, odc.year_week
ORDER BY odc.customer_id, odc.year_week
),

c_order AS (
SELECT *,
       DENSE_RANK() OVER (PARTITION BY r.customer_id ORDER BY r.cumulative_revenue) AS cumulative_order_lifetime
FROM revenue AS r
),

-- We then took a look at the date dimension and grouped orders by year week
-- We also wanted to find the end of the week on order to be able to filter by it later
year_week_date AS (
SELECT dd.year_week,
       MAX(date) AS end_of_week
FROM dimensions.date_dimension AS dd
GROUP BY dd.year_week
ORDER BY dd.year_week
),

-- we take c_order which is all of our revenue columns and combine it with the end of week variable using a left join of year week
final_revenue AS (
SELECT ywd.end_of_week,
       c.*
FROM c_order AS c
LEFT JOIN year_week_date AS ywd
ON c.year_week= ywd.year_week
),

-- we then look to combine our first order values columns with the first conversion columns.
-- This is done through a join on customer_id
first AS (
SELECT fo.first_order_number,
       fo.first_order_date,
       fo.first_order_week,
       fo.first_order_product,
       fo.first_order_unit_price,
       fo.first_order_discount,
       fo.first_order_price_paid,
       cs_fo.*
FROM conversions_with_first_orders AS cs_fo
LEFT JOIN first_order_values AS fo
    ON cs_fo.customer_id = fo.customer_id
ORDER BY cs_fo.customer_id
),

-- This combines all of the values we would need to display on our final table
first_and_revenue AS (
SELECT r.year_week,
       r.revenue,
       r.total_discount,
       r.cumulative_revenue,
       r.cumulative_order_lifetime,
       r.end_of_week,
       f.first_name,
       f.last_name,
       f.customer_id,
       f.conversion_id,
       f.conversion_number,
       f.order_number,
       f.conversion_date,
       f.next_conversion_date,
       f.conversion_type,
       f.order_date,
       f.conversion_channel,
       f.next_conversion_week,
       f.first_order_number,
       f.first_order_date,
       f.first_order_week,
       f.first_order_product,
       f.first_order_unit_price,
       f.first_order_discount,
       f.first_order_price_paid
FROM  final_revenue AS r
LEFT JOIN first AS f
    ON r.customer_id = f.customer_id AND r.end_of_week >= f.conversion_date
ORDER BY f.customer_id, f.conversion_date, r.year_week
),

-- We create one table where there is a next_conversion week
-- We also wanted to ensure we got rid of the null customer_id values we were getting
table1 AS (
SELECT *
FROM first_and_revenue
WHERE conversion_date < end_of_week
  AND next_conversion_date > end_of_week
  AND customer_id IS NOT NULL
),

-- We create a second table where there is no next_conversion week
-- Once again ensuring to remove null customer_id values
table2 AS (
SELECT *
FROM first_and_revenue
WHERE next_conversion_date IS NULL
  AND customer_id IS NOT NULL
),

-- We union those tables together, now we receive a table with all the rows we want
final_table AS (
SELECT *
FROM table1
UNION ALL
SELECT *
FROM table2
)

-- Now we just need to filter which column we want
-- In this process we must also filter for dates before year week 2023-W33 as no orders happen after that point
-- We also needed to order them correctly so the you saw the correct customer and conversion first. 
SELECT customer_id,
       first_name,
       last_name,
       conversion_id,
       conversion_number AS recurrence,
       conversion_type,
       conversion_date,
       next_conversion_week AS conversion_week,
       conversion_channel,
       next_conversion_week,
       first_order_number,
       first_order_date,
       first_order_week as first_order_week,
       first_order_product,
       first_order_unit_price,
       first_order_discount,
       first_order_price_paid,
       ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY year_week) AS week_counter,
       year_week AS order_week,
       CASE
           WHEN total_discount IS NOT NULL THEN (CASE WHEN revenue IS NOT NULL THEN revenue ELSE 0 END + total_discount)
       END AS grand_total,
       total_discount,
       CASE WHEN revenue IS NOT NULL THEN revenue ELSE 0 END AS total_paid,
       SUM(revenue) OVER(PARTITION BY conversion_id ORDER BY conversion_id, year_week) AS cum_revenue,
       SUM(revenue) OVER(ORDER BY conversion_id, year_week) AS cum_revenue_lifetime,
       DENSE_RANK() OVER (PARTITION BY conversion_id ORDER BY cumulative_order_lifetime) AS loyalty,
       cumulative_order_lifetime AS loyalty_lifetime
FROM final_table
WHERE year_week < '2023-W33'
ORDER BY customer_id, conversion_date, year_week;
