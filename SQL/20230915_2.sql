-- You've been provided with two tables in a PostgreSQL database:

-- users table with columns user_id (primary key), signup_date (date), and country (varchar)

-- purchases table with columns purchase_id (primary key), user_id (foreign key referencing users), purchase_date (date), and amount (decimal)

-- Your task is to write a SQL query that produces a report showing the total revenue (amount) generated from purchases for each country on a monthly basis. The report should have the following columns: country, month_year, and total_revenue.

-- Please note that month_year should be in the format 'YYYY-MM' and the countries should be listed in descending order based on the total_revenue.

WITH month_year_result AS (
    SELECT
        COALESCE(u.country, 'Unknown') as country
        , TO_CHAR(p.purchase_date, 'YYYY-MM') as month_year
        , SUM(p.amount) AS total_revenue
    FROM purchases p
    LEFT JOIN users u
    ON p.user_id = u.user_id
    WHERE p.amount >= 0
    GROUP BY u.country
            , TO_CHAR(p.purchase_date, 'YYYY-MM')
)
SELECT
    m.country
    , m.month_year
    , m.total_revenue
FROM month_year_result m
ORDER BY m.total_revenue DESC