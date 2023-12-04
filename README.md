### Сеть ресторанов Gastro Hub

Необходимо на основе сырых, необработанных данных построить дополнительные таблицы, представления и материализованные представления, а также написать несколько аналитических запросов.  

Базой для работы является дамп БД [sprint2_dump.sql](https://github.com/msmkdenis/SQL-Gastro-Hub/blob/main/sprint2_dump.sql)

Перед выполнением заданий необходимо создать ряд дополнительных таблиц и наполнить их данным.
Ниже приведены решения аналитических задач, полный скрипт решений приведен в файле [sprint_2.sql](https://github.com/msmkdenis/SQL-Gastro-Hub/blob/main/sprint_2.sql)

#### Задание 1
Создайте представление, которое покажет топ-3 заведений внутри каждого типа заведения по среднему чеку за все даты. 
Столбец со средним чеком округлите до второго знака после запятой.
```sql
create view cafe.top_3_cafe_per_type_by_average_check as
with
    average_check as (
        select
            cr.name,
            cr.type,
            avg(avg_check)::numeric(6,2) as average_check,
            ROW_NUMBER() over (partition by cr.type order by avg(avg_check) desc) as rank
        from cafe.sales cs
                 left join cafe.restaurants cr on cs.restaurant_uuid = cr.restaurant_uuid
        group by cr.name, cr.type)
select
    name as cafe_name,
    type as cafe_type,
    average_check
from average_check
where rank in (1,2,3);
```
#### Задание 2
Создайте материализованное представление, которое покажет, как изменяется средний чек для каждого заведения от года к году за все года за исключением 2023 года. 
Все столбцы со средним чеком округлите до второго знака после запятой.
```sql
create materialized view cafe.average_check_change_per_cafe_year as
with
    raw_check_per_year as(
        select
            restaurant_uuid,
            date,
            extract(year from date) as year,
            avg_check,
            avg(avg_check) over (partition by restaurant_uuid, extract(year from date)) as average_per_year
        from cafe.sales),

    check_per_year as(
        select
            distinct i.year,
                     cr.name,
                     cr.type,
                     i.average_per_year::numeric(6,2) as average_check_present_year
        from raw_check_per_year i
                 left join cafe.restaurants cr on i.restaurant_uuid = cr.restaurant_uuid
        order by cr.name, year)
select
    *,
    lag(average_check_present_year) over (partition by name) as average_check_previous_year,
    (((average_check_present_year - lag(average_check_present_year) over (partition by name))/
      lag(average_check_present_year) over (partition by name))*100)::numeric(6,2) as average_check_change
from check_per_year;
```
#### Задание 3
Найдите топ-3 заведения, где чаще всего менялся менеджер за весь период.
```sql
select
    r.name,
    count(rm.manager_uuid) as manager_change_times
from cafe.restaurant_manager_work_dates as rm
         join cafe.restaurants as r on rm.restaurant_uuid = r.restaurant_uuid
group by r.name
order by manager_change_times desc
limit 3;
```
#### Задание 4
Найдите пиццерию с самым большим количеством пицц в меню. Если таких пиццерий несколько, выведите все.
```sql
with
    pizzeria_list as (
        select
            name as name,
            jsonb_each(menu -> 'Пицца') as pizzas
        from cafe.restaurants
        where type = 'pizzeria'),

    pizzeria_list_rank as (
        select
            name,
            count(pizzas) as pizzas_quantity_menu,
            dense_rank() over(order by count(pizzas) desc) as rank
        from pizzeria_list
        group by name)
select
    name,
    pizzas_quantity_menu
from pizzeria_list_rank
where rank = 1;
```
#### Задание 5
Найдите самую дорогую пиццу для каждой пиццерии.
```sql
with
    pizzeria_price as (
        select
            name as pizzeria,
            jsonb_object_keys(menu -> 'Пицца') as pizza,
            ((menu -> 'Пицца') ->> (jsonb_object_keys(menu -> 'Пицца')))::numeric(6,0) as price
        from cafe.restaurants
        where type = 'pizzeria'),

    price_rank as (
        select
            *,
            ROW_NUMBER() OVER (partition by pc.pizzeria order by pc.price desc) as rank
        from pizzeria_price pc)
select
    pr.pizzeria,
    pr.pizza,
    pr.price
from price_rank pr
where rank = 1;
```
#### Задание 6
Найдите два самых близких друг к другу заведения одного типа.
```sql
select a.name as rest1,
       b.name as rest2,
       a.type as type,
       min(st_distance(a.location::geography, b.location::geography)) as distance
from cafe.restaurants a
         join cafe.restaurants b on a.type = b.type
where a.name::text <> b.name::text
group by a.name, b.name, a.type
order by (min(st_distance(a.location::geography, b.location::geography)))
limit 1;
```
#### Задание 7
Найдите район с самым большим количеством заведений и район с самым маленьким количеством заведений. 
Первой строчкой выведите район с самым большим количеством заведений, второй — с самым маленьким.
```sql
with
    district_coverage as (
        select
            district_name,
            name,
            st_covers(district_geom, location) as district_covers,
            count(case when st_covers(district_geom, location) then 1 end) over (partition by district_name) as covers_true,
            count(case when not st_covers(district_geom, location) then 1 end) over (partition by district_name) as covers_false
        from cafe.districts
                 cross join cafe.restaurants),

    max_coverage as (
        select
            district_name,
            covers_true
        from district_coverage
        order by covers_true desc
        limit 1),

    min_coverage as (
        select
            district_name,
            covers_true
        from district_coverage
        order by covers_true
        limit 1)

select
    district_name as district_name,
    covers_true as restaurants_quantity
from max_coverage
union all
select
    district_name,
    covers_true
from min_coverage;
```
