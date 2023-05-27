create database namaste_sql

use  namaste_sql

create table credit_card_transactions(
`index` int ,
city varchar(50) ,
`date` varchar(20) ,
card_type varchar(20) ,
exp_type varchar(20) ,
gender varchar(5) ,
amount int )

load data infile
'D:/Credit_card_transactions.csv'
into table credit_card_transactions
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows

select * from credit_card_transactions

-- exploring the dataset 

select min(date) , max(date) from credit_card_transactions
-- 2013 - 2015

select distinct card_type from credit_card_transactions
-- Gold
-- Platinum
-- Silver
-- Signature

select distinct exp_type from credit_card_transactions
-- Bills
-- Food
-- Entertainment
-- Grocery
-- Fuel
-- Travel

select distinct city from credit_card_transactions -- 986 rows 


-- 1- write a query to print top 5 cities with highest spends and 
-- their percentage contribution of total credit card spends 

with cte as 
(select city , sum(amount) as highest_spends  from 
credit_card_transactions 
group by city
order by highest_spends desc
limit 5)
select * , round (100 * highest_spends/(select sum(amount) from credit_card_transactions ), 2)
as percent_contribution
from cte  


-- 2- write a query to print highest spend month and amount spent in that month for each card type

with cte as 
(select year(date) as yr , month(date) as mon , card_type  , sum(amount) as total_spend
from credit_card_transactions
group by card_type , year(date) , month(date)
order by card_type , yr , mon)
, cte2 as 
(select * , row_number() over( partition by card_type order by total_spend desc) as row_rank
from cte)
select * from cte2 where row_rank = 1


-- 3- write a query to print the transaction details(all columns from the table) for each card type when
-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)

with cte as 
(select * , sum(amount) over(partition by card_type   order by date , `index`  )
as rolling_sum
from credit_card_transactions)
, cte2 as 
(select * , row_number() over (partition by card_type order by rolling_sum) as row_rank from cte
where rolling_sum > 1000000 )
select * from cte2 
where row_rank = 1


-- 4- write a query to find city which had lowest percentage spend for gold card type

with cte as 
(select city , card_type , sum(amount) as total_spend
from credit_card_transactions
group by city , card_type 
order by city , card_type)
, cte2 as 
(select city , sum(amount) as total_amount  from credit_card_transactions
group by city)
, cte3 as 
(select cte.city , card_type , total_spend  , total_amount
from cte
inner join cte2 
on cte.city = cte2.city)
, cte4 as 
 (select * , 100 * total_spend/total_amount as percent
from cte3 where card_type = "Gold") 
select * from cte4 
where percent = (select min(percent) from cte4)


-- 5- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type 
-- (example format : Delhi , bills, Fuel)

select left(b.city , position( ',' in b.city) - 1 ) as city_name , highest_expense_type , lowest_expense_type 
from 
(select city , exp_type as highest_expense_type from 
(select * , row_number() over(partition by city order by amount desc) as ran
from credit_card_transactions) a
where ran = 1) b
inner join
(select city , exp_type as lowest_expense_type from 
(select * , row_number() over(partition by city order by amount ) as ran
from credit_card_transactions) a
where ran = 1) c 
on b.city = c.city;


-- 6- write a query to find percentage contribution of spends by females for each expense type

select * , round(100 * total_female_amount/total_amount , 1) as percent_contribution 
from  (with cte1 as 
(select exp_type , sum(amount) as total_amount
from credit_card_transactions
group by exp_type)
, cte2 as 
(select exp_type , sum(amount) as total_female_amount 
from credit_card_transactions 
where gender = "F" 
group by exp_type)
select cte1.exp_type , total_amount , total_female_amount
from cte1
inner join cte2
on cte1.exp_type = cte2.exp_type ) a;


-- 7- which card and expense type combination saw highest month over month percent growth in Jan-2014

with cte as
(select month(date) as month , year(date) as year , card_type ,  exp_type , sum(amount) as cur_mon_sales
from credit_card_transactions
group by card_type , exp_type , year(date) , month(date)
order by  card_type  , exp_type  , year(date) ,month(date)) 
, cte2 as 
(select * , lag(cur_mon_sales ,1) over(partition by card_type , exp_type ) as prv_mon_sales
from cte )
, cte3 as 
(select * , 100 * (cur_mon_sales - prv_mon_sales)/prv_mon_sales as percent_growth
from cte2
where month = 1 and year = 2014)
select card_type , exp_type , percent_growth 
from cte3 where percent_growth =(select max(percent_growth) from cte3);


-- 9- during weekends which city has highest total spend to total no of transcations ratio 

with cte as 
(select * ,  dayname(date) as `week` 
from credit_card_transactions)
, cte2 as 
(select city , sum(amount) as total_amount , count(*) as total_transactions
from cte 
where `week` in ("Saturday" , "Sunday")
group by city)
, cte3 as 
(select *  , total_amount/total_transactions as ratio
from cte2)
select city from cte3
where ratio = (select max(ratio) from cte3);


-- 10- which city took least number of days to reach its 500th transaction after the first transaction in that city

select city , last_date , first_date , datediff(last_date , first_date) as days_took
from 
(with cte as
(select * , row_number() over(partition by city order by date) as row_rank 
from credit_card_transactions)
select a.city , a.date as last_date , b.date as first_date  
from  
(select city , date , row_rank
from cte 
where row_rank = 500) a
inner join 
(select city , date , row_rank
from cte 
where row_rank = 1) b
on a.city = b.city) a
order by days_took 
limit 1
