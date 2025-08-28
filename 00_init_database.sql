/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM '/var/opt/mssql/import/csv-files/gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM '/var/opt/mssql/import/csv-files/gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO


TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM '/var/opt/mssql/import/csv-files/gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

select * from gold.fact_sales;

select year(order_date) as order_year,
sum(sales_amount) as amt,
count(distinct customer_key) as customer_count,
sum(quantity) as total_quantity
from gold.fact_sales
where year(order_date) is not null
group by year(order_date)
order by year(order_date);

select 
DATETRUNC (MONTH,order_date) as order_year,
sum(sales_amount) as amt,
count(distinct customer_key) as customer_count,
sum(quantity) as total_quantity
from gold.fact_sales
where year(order_date) is not null
group by DATETRUNC (MONTH,order_date)
order by DATETRUNC (MONTH,order_date);

select 
FORMAT(order_date,'yyyy-MM') as order_year,
sum(sales_amount) as amt,
count(distinct customer_key) as customer_count,
sum(quantity) as total_quantity
from gold.fact_sales
where year(order_date) is not null
group by FORMAT(order_date,'yyyy-MM')
order by FORMAT(order_date,'yyyy-MM');

-- Cummulative query / moving average over time

with cte as (
select DATETRUNC(MONTH,order_date) as order_month,
sum(sales_amount) as total_amount
from gold.fact_sales
where order_date is not null
group by DATETRUNC(MONTH,order_date), DATETRUNC(YEAR,order_date) 
)
select order_month,total_amount, 
sum(total_amount) over(order by order_month) as running_total_sales
from cte
order by DATETRUNC(MONTH,order_month) ;


with cte as (
select DATETRUNC(YEAR,order_date) as order_year,
AVG(price) as average_price, 
sum(sales_amount) as total_amount
from gold.fact_sales
where order_date is not null
group by DATETRUNC(YEAR,order_date) 
)
select order_year,total_amount,
AVG(average_price) over(order by order_year) as moving_average, 
sum(total_amount) over(order by order_year) as running_total_sales
from cte
order by DATETRUNC(YEAR,order_year) ;



with cte as (
select YEAR(f.order_date) AS order_YEAR,
p.product_name,
sum(f.sales_amount) as total_sales
from gold.fact_sales f 
left join gold.dim_products p
on f.product_key=p.product_key
where order_date is not null
group by YEAR(f.order_date),p.product_name)

select order_YEAR, product_name,total_sales,
avg(total_sales) over(partition by product_name ) as avg_sales,
total_sales- avg(total_sales) over(partition by product_name ) as diff_sales,
lag(total_sales) over(partition by product_name order by order_YEAR) as prev_sales,
case when total_sales- avg(total_sales) over(partition by product_name ) <0 then 'Below_Avg'
when total_sales- avg(total_sales) over(partition by product_name ) >0 then 'Above_Avg'
else 'Avg' end as avg_flag,
case when  lag(total_sales) over(partition by product_name order by order_YEAR)<0 then 'Decrease sales from last year'
when lag(total_sales) over(partition by product_name order by order_YEAR) >0 then 'Increase sales from last year'
else 'No change' end as diff_flag
from cte
order by product_name,order_YEAR;

with cte as(
SELECT sum(f.sales_amount) as total_sales , p.category from  gold.fact_sales f  left join 
gold.dim_products p on f.product_key=p.product_key
group by p.category)
select total_sales, category, sum(total_sales) over() as overall_sales,
concat(round((cast (total_sales as float)/sum(total_sales) over()) *100,2),'%') as percentage_of_total
from cte 
order by total_sales desc
;

-- segment products into cost ranges and count how many products fall into each segment
with cte as (
select product_key,
product_name,cost,
case when cost <100 then 'Below 100'
when cost between 100 and 500 then '100-500'
when cost between 500 and 1000 then '500-1000'
else 'Above 1000' end as segments 
from gold.dim_products)
select segments,count(product_key) as total_count from cte 
group by cte.segments
order by count(product_key);

with cte as(select 
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
DATEDIFF(MONTH,min(order_date),max(order_date)) as lifespan
from gold.fact_sales f left join 
gold.dim_customers c ON
f.customer_key=c.customer_key
group by c.customer_key),
cte1 as (
select customer_key, total_spending, lifespan,
case when lifespan >=12 and total_spending > 5000 then 'VIP'
when lifespan >=12 and total_spending <= 5000 then 'Regular'
else 'New' end as customer_segment
from cte)
select customer_segment, count(customer_key)
from cte1 
group by customer_segment;



--Reports
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO

CREATE VIEW gold.report_customers
AS
with base_query as (
select concat(c.first_name,' ',c.last_name) as customer_name,
c.customer_key,
f.sales_amount,f.quantity,f.product_key,
f.order_number,f.order_date,
DATEDIFF(year,c.birthdate,GETDATE()) as age,
c.customer_number
from gold.fact_sales f LEFT join gold.dim_customers c 
on c.customer_key=f.customer_key
where order_date is not null)
, customer_aggregation as (
select
customer_name,
customer_key, 
 age,
 customer_number,
 count(distinct order_number) as total_orders,
 sum(sales_amount) as total_sales,
 sum(quantity) as total_quantity,
 count(distinct product_key) as total_product,
max(order_date) as last_order,
 DATEDIFF(MONTH,min(order_date),max(order_date)) as lifespan
from base_query
group by 
customer_key, 
 customer_number ,
 customer_name,
 age)
 
 select 
 customer_key, 
 customer_number ,
 customer_name,
 age,
 case 
 	when age < 20 then 'Below 20'
 	when age between 20 and 29 then '20-29' 
 	when age between 30 and 39 then '30-39'
 	when age between 40 and 49 then '40-49'
 else 'Above 50' 
 end as age_group,
 case 
 	when lifespan >=12 and total_sales > 5000 then 'VIP'
	when lifespan >=12 and total_sales <= 5000 then 'Regular'
else 'New' 
end as customer_segment,
 last_order,
DATEDIFF(MONTH, last_order, GETDATE()) as recency,
total_orders,
total_sales,
total_quantity,
total_product ,
case when total_orders=0 then 0 else total_sales/total_orders end as avg_order_value ,
case when lifespan =0 then total_sales 
else total_sales/lifespan end as avg_monthly_spend
from customer_aggregation;


IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO

CREATE VIEW gold.report_products
AS
with base_query as (
select p.product_key,p.product_name, p.category, p.subcategory,p.cost,
f.sales_amount,f.quantity,f.customer_key,
f.order_number,f.order_date
from gold.fact_sales f LEFT join gold.dim_products p 
on p.product_key=f.product_key
where order_date is not null)
,product_aggregation as (
select
product_key,
product_name,
category, 
subcategory,
cost,
count(distinct order_number) as total_orders,
count(distinct customer_key) as total_customers,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_product,
max(order_date) as last_sale_date,
DATEDIFF(MONTH,min(order_date),max(order_date)) as lifespan,
round(AVG(Cast(sales_amount as FLOAT)/NULLIF(quantity,0)),1) as avg_sales_price
from base_query
group by 
product_key,
product_name,
category, 
subcategory,
cost)
 
 select 
product_key,
product_name,
category, 
subcategory,
cost,
 case 
 	when total_sales> 50000 then 'High-Performer'
	WHEN total_sales>= 10000 then 'Mid-Performer'
 else 'Low-Performer' 
 end as product_segment,
 last_sale_date,
DATEDIFF(MONTH, last_sale_date, GETDATE()) as recency,
total_customers,
total_orders,
total_sales,
total_quantity,
total_product ,
avg_sales_price,
lifespan,
case when total_orders=0 then 0 else total_sales/total_orders end as avg_order_value ,
case when lifespan =0 then total_sales 
else total_sales/lifespan end as avg_monthly_spend
from product_aggregation;

