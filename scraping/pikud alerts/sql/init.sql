-- create or replace cities 
DROP TABLE IF EXISTS cities;
create table cities (
label varchar(255),
value varchar(255),
id varchar(255),
areaid int,
areaname varchar(255),
label_he varchar(255),
migun_time int);

-- create or replace alerts
DROP TABLE IF EXISTS alerts;
create table alerts (
data varchar(4000),
date varchar(255),
time varchar(255),
alertDate varchar(255),
category int,
category_desc varchar(255),
matrix_id int,
rid int,
NAME_HE varchar(255),
NAME_EN varchar(255),
NAME_AR varchar(255),
NAME_RU varchar(255));

-- create alerts_v view
create or replace view alerts_v as 
with days as (select day_date, concat('Day ', convert(row_number() over (), char)) as day_desc
				from(
					select distinct date as day_date
					from alerts
					order by 1
					) as x),
	area as (select distinct label city_name, areaname
				from cities)
## --------------------------					
##        Main query
## --------------------------
select * , case when name_en_full like '%Ashkelon%' Then 'Ashkelon' 
			when name_en_full like '%Ashdod%' Then 'Ashdod'
			when name_en_full like '%Degania%' then 'Degania'
			when name_en_full like '%Dimona%' then 'Dimona'
			when name_en_full like '%Hatzor HaGlilit%' then 'Hatzor HaGlilit'
			when name_en_full like '%Kiryat Gat%' then 'Kiryat Gat'
			when name_en_full like '%Kannot%' then 'Kannot'
			when name_en_full like '%Timorim%' then 'Timorim'
			when name_en_full like '%Tkuma%' then 'Tkuma'
	   else name_en_full end name_en
from (
select 
d.day_desc, 
ar.areaname, 
convert(a.alertDate, datetime) alert_date, a.category, a.category_desc, 
case when trim(a.name_he) like '% - %' then substr(trim(a.name_he), 1, instr(trim(a.name_he), ' - ')-1) else trim(a.name_he) end as name_he,
name_en as name_en_orig,
case when trim(a.name_en) like '% - %' then substr(trim(a.name_en), 1, instr(trim(a.name_en), ' - ')-1) else COALESCE(replace(a.NAME_EN, '', null), a.data) end as name_en_full
-- case when trim(a.name_ar) like '% - %' then substr(trim(a.name_ar), 1, instr(trim(a.name_ar), ' - ')-1) else trim(a.name_ar) end as name_ar,
-- case when trim(a.name_ru) like '% - %' then substr(trim(a.name_ru), 1, instr(trim(a.name_ru), ' - ')-1) else trim(a.name_ru) end as name_ru
from alerts a 
left join days d
on a.date = d.day_date
left join area ar
on trim(a.data) = trim(ar.city_name)) m
where m.category = 1