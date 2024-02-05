delete from products where created = current_date;
insert into products 
select product_number, product_name, product_desc, type_name, colors, price, prev_price, units, url, online_sell, last_chance, availability, is_breath_taking, created
from (
    select distinct product_number, product_name, product_desc, type_name, colors, price, prev_price, units, url, online_sell, last_chance, availability, 
    is_breath_taking , created,
    row_number() over (partition by product_number, product_name, product_desc, type_name, colors, price, prev_price, units, online_sell, last_chance, availability, 
    is_breath_taking , created  order by url) as rn
    from mrr_products
    where created = current_date) as prod
where rn = 1;