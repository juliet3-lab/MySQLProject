--write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends

with cte1 as (select city,card_type,amount,(select sum(amount)from CC_Trans) total_spend from CC_Trans)

,cte2 as(select distinct city,total_spend,sum(amount) over(partition by city) per_city_spends from cte1)

select top 5 city,per_city_spends,round((per_city_spends/total_spend)*100,2) percentage_contribution from cte2

order by per_city_spends desc;

--write a query to print highest spend month and amount spent in that month for each card type

with cte1 as(select distinct card_type,datepart(month,transaction_date) as months,datepart(year,transaction_date) as years,sum(amount) over(partition by card_type,datepart(month,transaction_date),datepart(year,transaction_date))  as spends_per_month from CC_Trans
)
, cte2 as(select *,rank() over(partition by card_type order by card_type,spends_per_month desc) as rw_num from cte1)

select card_type,months,years,spends_per_month from cte2

where rw_num = 1;

--write a query to print the transaction details(all columns from the table) for each card type when it reaches a cumulative of 1000000 total spends
with cte1 as (
select *,sum(amount) over(partition by card_type order by transaction_date,transaction_id) as total_spend

from CC_Trans
)
select * from (select *, rank() over(partition by card_type order by total_spend) as rn 

from cte1 where total_spend >= 1000000) a where rn=1

--write a query to find city which had lowest percentage spend for gold card type

with cte1 as(select card_type,city,amount,sum(amount) over(partition by card_type) per_card from CC_Trans

where card_type = 'Gold')

,cte2 as(select city,card_type,per_card,sum(amount) over(partition by city) amount

from cte1

where card_type = 'Gold')

select top 1*,round((amount/per_card)*100,2) as percent_ from cte2;

--write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type

with cte1 as(select distinct city,exp_type,sum(amount) over(partition by exp_type,city) spends_on_exp_type from CC_Trans)

,cte2 as(select city,exp_type,spends_on_exp_type,rank() over(partition by city order by spends_on_exp_type desc)rnk1

,rank() over(partition by city order by spends_on_exp_type asc) rnk2 from cte1)

select city,max(case when rnk1 = 1 then exp_type end) as highest_exp_type,max(case when rnk2 = 1 then exp_type end) as lowest_exp_type from cte2

group by city;

--write a query to find percentage contribution of spends by females for each expense type

select exp_type, sum(case when gender = 'F' then (amount) end)/sum(amount) as percentage_contribution_women

from CC_Trans

group by exp_type

order by percentage_contribution_women desc

--which card and expense type combination saw highest month over month growth in Jan-2014

with cte1 as(select distinct card_type,exp_type,datepart(month,transaction_date) as mon, datepart(year,transaction_date) as yr

,sum(amount) over(partition by card_type,exp_type,datepart(year,transaction_date) order by datepart(month,transaction_date),datepart(year,transaction_date)) as total_spend from CC_Trans

where datepart(month,transaction_date) = 12 and datepart(year,transaction_date) = 2013 or datepart(month,transaction_date) = 1 and datepart(year,transaction_date) = 2014)

,cte2 as (select *,lag(total_spend,1) over(partition by card_type,exp_type order by yr ) as prev_mon_spend  from cte1)

select top 1 card_type,exp_type,mon,yr,total_spend, (total_spend-prev_mon_spend) mom_growth from cte2

order by mom_growth desc

--during weekends which city has highest total spend to total no of transcations ratio 

select top 1 city,sum(amount)/count(transaction_date) as ratio

from CC_Trans

where DATEPART(WEEKDAY,transaction_date) in('1','7')

group by city

order by ratio desc

--which city took least number of days to reach its 500th transaction after the first transaction in that city

with cte1 as(select city,transaction_date,row_number() over(partition by city order by transaction_date ) rw_numb from CC_trans)

,cte2 as(select city,max(case when rw_numb =1 then transaction_date end) as starting_date

,max(case when rw_numb =500 then transaction_date end) as end_date from cte1

group by city)

select top 1 city,datediff(day,starting_date,end_date) as least_days_to_reach_500th_trnx from cte2

where datediff(day,starting_date,end_date) is not null

order by least_days_to_reach_500th_trnx asc















