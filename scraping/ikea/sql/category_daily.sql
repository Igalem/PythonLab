delete from category_daily where created = current_date;
insert into category_daily
select category , 
		created , 
		count(distinct product_number) as products, 
		count(distinct case when tag = 'NEW_PRODUCT' then product_number end) as new_products,
		round(avg(price), 2) as avg_price, 
		sum(is_breath_taking) price_changed, 
		count(distinct colors) colors
from mrr_products mp  
where created  = current_date
group by category, created 
order by count(distinct product_number) desc;