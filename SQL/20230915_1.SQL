-- Consider a database table orders that has the following columns:

-- id (integer): the unique identifier for each order
-- customer_id (integer): the unique identifier for each customer
-- order_date (date): the date when the order was placed
-- total_price (float): the total price of the order
-- Now, suppose we want to find the top 3 customers who have spent the most money on orders in the year 2023.

-- Write a SQL query to find these customers. For each customer, we want their customer_id and the total amount they have spent on orders in 2023.

-- using Hive SQL
WITH price_per_cust AS (
    SELECT
        customer_id
        , SUM(total_price) AS sum_total_price
    FROM orders o
    WHERE year(o.order_date) = '2023'
    GROUP BY customer_id
)
SELECT
    customer_id
    , sum_total_price
FROM price_per_cust
ORDER BY sum_total_price
LIMIT 3


-- The total_price for each order is positive. If refunds or other negative amounts can be included, this might affect the output.