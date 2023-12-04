--ШАГ 1:
create type cafe.restaurant_type as enum
    ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

--ШАГ 2:
create table if not exists cafe.restaurants (
    restaurant_uuid uuid default gen_random_uuid(),
    name varchar(50) not null,
    location geometry not null,
    type cafe.restaurant_type not null,
    menu jsonb not null,
    constraint pk_restaurants primary key (restaurant_uuid)
);

insert into cafe.restaurants
    (name, location, type, menu)
select
    distinct s.cafe_name,
             ST_SetSRID(st_point(s.longitude, s.latitude),4326),
             s.type::cafe.restaurant_type,
             m.menu
from raw_data.sales s
left join raw_data.menu m on s.cafe_name = m.cafe_name;

--ШАГ 3:
create table if not exists cafe.managers (
    manager_uuid uuid default gen_random_uuid(),
    name varchar(50) not null,
    phone varchar(50) not null,
    constraint pk_managers primary key (manager_uuid)
);

insert into cafe.managers
    (name, phone)
select
    distinct manager,
             manager_phone
from raw_data.sales;

--ШАГ 4:
create table if not exists cafe.restaurant_manager_work_dates (
    restaurant_uuid uuid not null,
    manager_uuid uuid not null,
    working_start_date date not null,
    working_end_date date,
    constraint pk_restaurant_manager_work_dates primary key (restaurant_uuid, manager_uuid),
    constraint fk_restaurant foreign key (restaurant_uuid) references cafe.restaurants,
    constraint fk_manager foreign key (manager_uuid) references cafe.managers
);

insert into cafe.restaurant_manager_work_dates
    (restaurant_uuid, manager_uuid, working_start_date, working_end_date)
select
    r.restaurant_uuid as restaurant_uuid,
    m.manager_uuid as manager_uuid,
    min(report_date) as start_work,
    max(report_date) as end_work
from raw_data.sales as s
         join cafe.restaurants as r on s.cafe_name = r.name
         join cafe.managers as m on s.manager = m.name
group by 1,2;

--ШАГ 5:
create table if not exists cafe.sales (
    date date not null,
    restaurant_uuid uuid not null,
    avg_check numeric(6,2),
    constraint pk_sales primary key (date, restaurant_uuid),
    constraint fk_restaurant foreign key (restaurant_uuid) references cafe.restaurants
);

insert into cafe.sales
    (date, restaurant_uuid, avg_check)
select
    rds.report_date,
    cr.restaurant_uuid,
    rds.avg_check
from raw_data.sales rds
    left join cafe.restaurants cr on rds.cafe_name = cr.name;

--ЗАДАНИЕ 1
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

--ЗАДАНИЕ 2
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

--ЗАДАНИЕ 3
select
    r.name,
    count(rm.manager_uuid) as manager_change_times
from cafe.restaurant_manager_work_dates as rm
         join cafe.restaurants as r on rm.restaurant_uuid = r.restaurant_uuid
group by r.name
order by manager_change_times desc
limit 3;

--ЗАДАНИЕ 4
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

--ЗАДАНИЕ 5
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

--ЗАДАНИЕ 6
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

--ЗАДАНИЕ 7
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