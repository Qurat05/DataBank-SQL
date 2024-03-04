


-- PORTFOLIO PROJECT: DATA BANK

SELECT * FROM customer_nodes;
SELECT * FROM regions;
SELECT * FROM customer_transactions;

-- A. Customer Nodes Exploration
-- 1. How many unique nodes are there on the Data Bank system?
SELECT COUNT(DISTINCT(node_id)) as count_nodes
FROM customer_nodes;

-- 2. What is the number of nodes per region?
SELECT r.region_id, r.region_name, COUNT(DISTINCT(cn.node_id)) AS No_of_Nodes
FROM customer_nodes
AS cn
JOIN regions
AS r
ON r.region_id = cn.region_id
GROUP BY 1,2
ORDER BY region_id ASC;

-- 3. How many customers are allocated to each region?

SELECT r.region_id, r.region_name, COUNT(DISTINCT(cn.customer_id)) AS total_customers
FROM customer_nodes
AS cn
JOIN regions
AS r
ON cn.region_id = r.region_id
GROUP BY 1,2;

-- 4. How many days on average are customers reallocated to a different node?
WITH date_difference
AS (
	SELECT node_id, DATEDIFF(end_date,start_date) AS date_diff
    FROM customer_nodes
    WHERE end_date NOT LIKE '%9999%'
	) 
SELECT node_id, CONCAT(ROUND(AVG(date_diff),2),' days')  as Avg_days 
FROM date_difference
GROUP BY node_id
ORDER BY node_id ASC;


-- 5. What is the median, 80th and 95th percentile for this same reallocation 
-- days metric for each region?


-- a. Median, 80th, 95th percentile
-- b. reallocation days
-- c. for each region

WITH rows_ as (
SELECT c.customer_id,
r.region_name, DATEDIFF(c.end_date, c.start_date) AS days_difference,
row_number() over (partition by r.region_name order by DATEDIFF(c.end_date, c.start_date)) AS rows_number,
COUNT(*) over (partition by r.region_name) as total_rows  
from
customer_nodes c JOIN regions r ON c.region_id = r.region_id
where c.end_date not like '%9999%'
)
SELECT region_name,
ROUND(AVG(CASE WHEN rows_number between (total_rows/2) and ((total_rows/2)+1) THEN days_difference END), 0) AS Median,
MAX(CASE WHEN rows_number = round((0.80 * total_rows),0) THEN days_difference END) AS Percentile_80th,
MAX(CASE WHEN rows_number = round((0.95 * total_rows),0) THEN days_difference END) AS Percentile_95th
from rows_
group by region_name;

                    
-- B. Customer Transactions
-- 1. What is the unique count and total amount for each transaction type?
-- a. Unique count for each transaction type
-- b. Total amount for each transaction type

SELECT txn_type, count(DISTINCT(customer_id)) as TotalCount, SUM(txn_amount) as TotalAmount
FROM customer_transactions
GROUP BY 1;

-- 2. What is the average total historical deposit counts and amounts for all
-- 	  customers?
-- a. to find total counts for deposit
-- b. to find Avg(sum of total amounts)


SELECT ROUND(AVG(deposit_count)) AS avg_deposit_count, 
ROUND(AVG(deposit_amount)) AS avg_deposit_amount
FROM
(SELECT customer_id, COUNT(txn_type) AS deposit_count, SUM(txn_amount) AS deposit_amount
FROM customer_transactions
WHERE txn_type = 'deposit'
GROUP BY customer_id) deposit_txns;

-- 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase 
-- or 1 withdrawal in a single month?
# To find number of customers using service type more than 1 times for each month.

WITH updated_transactions AS (
SELECT customer_id,
	COUNT(CASE WHEN txn_type='deposit' THEN customer_id END) AS deposit_count,
	COUNT(CASE WHEN txn_type='purchase' THEN customer_id END) AS purchase_count,
    COUNT(CASE WHEN txn_type='withdrawal' THEN customer_id END) AS withdrawal_count
FROM customer_transactions
GROUP BY customer_id)
-- Main Query: To find total number of customers per month.
SELECT MONTHNAME(txn_date) AS month,
CONCAT(COUNT(DISTINCT t.customer_id), ' customers' ) AS customers_count
FROM updated_transactions 
AS t
JOIN customer_transactions
AS c
ON c.customer_id = t.customer_id
WHERE t.deposit_count> 1 AND (t.purchase_count > 0 OR t.withdrawal_count > 0)
GROUP BY 1
ORDER BY 2 DESC;

-- January had the highest amount of returning customers.

-- 4. What is the closing balance for each customer at the end of the month?
-- closing balance for each customer
-- at end of the month
WITH monthly_balance AS (
SELECT customer_id,
MONTHNAME(txn_date) AS month,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS closing_balance
FROM customer_transactions
GROUP BY 1,2
)
SELECT customer_id, month, closing_balance
FROM monthly_balance
ORDER BY 1;

-- 5. What is the percentage of customers who increase their closing balance
-- by more than 5%?
-- To find sum of txn_amount by each month for each customer		  - CTE1	
-- To perform window function to find balance change of each customer - CTE2
-- To compare balance change with 5%        						  - CTE3
-- To find total percentage of customer having closing balance > 5%   - Main Query  

-- CTE-1: To find sum of txn_amount for each customer per month
WITH monthly_balance AS (
SELECT customer_id,
EXTRACT(YEAR FROM txn_date) AS txn_year,
EXTRACT(MONTH FROM txn_date) AS txn_month,
SUM(txn_amount) OVER(PARTITION BY customer_id 
ORDER BY EXTRACT(YEAR FROM(txn_date)), EXTRACT(MONTH FROM(txn_date))) AS closing_balance
FROM customer_transactions
)
-- CTE-2: To find percent change from previous month
, monthly_balance_change AS (
SELECT customer_id, txn_year, txn_month,closing_balance,
LAG(closing_balance,1) OVER(PARTITION BY customer_id ORDER BY txn_year,txn_month),
(closing_balance-LAG(closing_balance,1) OVER(PARTITION BY customer_id ORDER BY txn_year, txn_month))
/ LAG(closing_balance,1) OVER(PARTITION BY customer_id ORDER BY txn_year,txn_month) AS balance_change
FROM monthly_balance
),
-- CTE-3: To find customers having balance change > 5%
customers_with_increase AS (
SELECT customer_id
FROM monthly_balance_change
WHERE balance_change > 0.05
GROUP BY customer_id
)
-- Main_Query: To find percentage of total customers having balance change > 5% 
SELECT 
(CAST(COUNT(DISTINCT customer_id) AS DECIMAL))/
CAST((SELECT COUNT(DISTINCT customer_id) FROM customer_transactions) AS DECIMAL) *  100 AS Percent_increase_in_customers
FROM customers_with_increase;
                                                            
 
-- C. Data Allocation Challenge
-- 1. Running balance at the end of each previous month
-- To find running balance of each customer based on their order of transaction.
-- To adjust txn_amount with +ve amount for deposit and -ve amount for purchase or withdrawal

-- CTE 1
WITH adjusted_transactions AS (
SELECT customer_id, txn_type, txn_amount,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type IN ('withdrawal','purchase') THEN -txn_amount
ELSE 0
END AS updated_transactions
FROM customer_transactions),
-- CTE 2
	closing_balance AS (
	SELECT customer_id, txn_type, updated_transactions,
	SUM(updated_transactions) OVER(PARTITION BY customer_id ORDER BY customer_id) as closing_balance
	FROM adjusted_transactions
	)
SELECT * FROM closing_balance;

-- Closing Balance Grouped by each customers

WITH adjusted_transactions AS (
SELECT customer_id, txn_type, txn_amount,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type IN ('withdrawal','purchase') THEN -txn_amount
ELSE 0
END AS updated_transactions
FROM customer_transactions),
closing_balance AS (
SELECT customer_id, txn_type, updated_transactions,
SUM(updated_transactions) OVER(PARTITION BY customer_id ORDER BY customer_id) as closing_balance
FROM adjusted_transactions
)
SELECT customer_id, closing_balance
 FROM closing_balance
GROUP BY customer_id,2;


SELECT customer_id,
       txn_date,
       SUM(txn_amount) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions;

-- 2. Customer Balance at the End of Each month
-- To find closing balance for each customer for each month.
-- Adjust the txn_amount to set values -ve for the payment and withdrawal.

WITH adjusted_amount AS (
SELECT customer_id, txn_amount,
EXTRACT(MONTH FROM(txn_date)) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type IN ('withdrawal','purchase') THEN -txn_amount
ELSE 0
END AS amount_update
FROM customer_transactions
),
closing_balance AS (
SELECT customer_id, month, SUM(amount_update) AS closing_amount
FROM adjusted_amount
GROUP BY 1,2
)
SELECT * FROM closing_balance
ORDER BY customer_id,month;


-- 3. To find Min, Avg, Max values for running balance for each customer.
SELECT * FROM customer_transactions; 

WITH adjusted_amount AS (
SELECT customer_id, txn_date, txn_type, txn_amount,
CASE 
WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type IN ('withdrawal','purchase') THEN -txn_amount
ELSE 0
END AS updated_amount
FROM customer_transactions
),
total_amount AS(
SELECT customer_id, txn_date, txn_type, 
SUM(updated_amount) OVER(PARTITION BY customer_id ORDER BY customer_id) AS total_amount
FROM adjusted_amount
)
SELECT customer_id, txn_date,
MIN(total_amount),
AVG(total_amount),
MAX(total_amount)
FROM total_amount
GROUP BY customer_id;


-- 3. Min, Max, Avg Running Balance
WITH running_balances AS (
  SELECT customer_id,
         SUM(txn_amount) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
  FROM customer_transactions
)
SELECT customer_id,
       MIN(running_balance) AS Min_running_balance,
       AVG(running_balance) AS Avg_running_balance,
       MAX(running_balance) AS Max_running_balance
FROM running_balances
GROUP BY customer_id;
 

-- Option 1: Data is allocated based off the amount of money at the end of the previous month?

SET SQL_mode = '';

WITH adjusted_amount AS (
SELECT customer_id, txn_type, 
EXTRACT(MONTH FROM (txn_date)) AS month_number, 
MONTHNAME(txn_date) AS month,
CASE 
WHEN  txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month_number, month,
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY month_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
AS running_balance
FROM adjusted_amount
),
allocation AS (
SELECT customer_id, month_number,month,
LAG(running_balance,1) OVER(PARTITION BY customer_id, month_number ORDER BY month_number) AS monthly_allocation
FROM balance
)
SELECT month_number,month,
SUM(CASE WHEN monthly_allocation < 0 THEN 0 ELSE monthly_allocation END) AS total_allocation
FROM allocation
GROUP BY 1,2
ORDER BY 1,2; 
 
-- Option 2: Data is allocated on the average amount of money kept in the
-- account in the previous 30 days

WITH updated_transactions AS (
SELECT customer_id, txn_type, 
EXTRACT(MONTH FROM(txn_date)) AS Month_number,
MONTHNAME(txn_date) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month, month_number,
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY customer_id, month_number 
ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM updated_transactions
),

avg_running AS(
SELECT customer_id, month,month_number,
AVG(running_balance) AS avg_balance
FROM balance
GROUP BY 1,2,3
ORDER BY 1

)
SELECT month_number,month, 
SUM(CASE WHEN avg_balance < 0 THEN 0 ELSE avg_balance END) AS allocation_balance
FROM avg_running
GROUP BY 1,2
ORDER by 1,2;


-- Option 3: Data is updated real-time
WITH updated_transactions AS (
SELECT customer_id, txn_type,
EXTRACT(MONTH FROM(txn_date)) AS month_number,
MONTHNAME(txn_date) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month_number, month, 
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY customer_id, month_number ASC 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM updated_transactions
)
SELECT month_number, month,
SUM(CASE WHEN running_balance < 0 THEN 0 ELSE running_balance END) AS total_allocation
FROM balance
GROUP BY 1,2
ORDER BY 1;

-- D. Extra Challenge
-- To calculate the data growth using an interest calculation
-- Annual interest rate = 6%
-- Data required on monthly basis

WITH adjusted_amount AS (
SELECT customer_id, 
EXTRACT(MONTH FROM(txn_date)) AS month_number,
MONTHNAME(txn_date) AS month,
SUM(CASE 
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END) AS monthly_amount
FROM customer_transactions
GROUP BY 1,2,3
ORDER BY 1
),
interest AS (
SELECT customer_id, month_number,month, monthly_amount,
ROUND(((monthly_amount * 6 * 1)/(100 * 12)),2) AS interest
FROM adjusted_amount
GROUP BY 1,2,3,4
ORDER BY 1,2,3
),
total_earnings AS (
SELECT customer_id, month_number, month,
(monthly_amount + interest) as earnings
FROM  interest
GROUP BY 1,2,3,4
ORDER BY 1,2,3
)
SELECT month_number,month,
SUM(CASE WHEN earnings < 0 THEN 0 ELSE earnings END) AS allocation
FROM total_earnings
GROUP BY 1,2
ORDER BY 1,2;





SELECT * FROM customer_nodes;
SELECT * FROM regions;
SELECT * FROM customer_transactions;

