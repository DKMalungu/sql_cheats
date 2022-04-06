-- Example data source id the postgres DVD Rental Dataset
-- link: https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/

-- Postgres Inner Join
-- Using inner join to join two tables
select address_id,
       address,
       address2,
       district,
       a.city_id,
       postal_code,
       phone,
       a.last_update
from address as a
inner join city c on c.city_id = a.city_id
order by address;

select address_id,
       address,
       address2,
       district,
       a.city_id,
       postal_code,
       phone,
       a.last_update
from address as a
         inner join city c using(city_id)
order by address;

-- Using inner join to join Three tables
select *
from address as a
inner join city c using(city_id)
inner join country cc on c.country_id = cc.country_id
order by address;

-- Postgres Left Join Clause
select *
from film f
         left join inventory i on f.film_id = i.film_id
order by title;

-- Postgres Left Outer Join Clause
select *
from film f
left join inventory i on f.film_id = i.film_id
where i.inventory_id is null
order by title;

-- Postgres Right Join Clause
select *
from language l
right join film f on l.language_id = f.language_id;

select *
from inventory i
right join rental r on i.inventory_id = r.inventory_id;

select *
from film f
         right join inventory i on f.film_id = i.film_id
order by title;

-- Postgres Right Outer Join Clause
select *
from language l
         right join film f on l.language_id = f.language_id
where l.language_id is null;

select *
from inventory i
         right join rental r on i.inventory_id = r.inventory_id
where i.inventory_id is null;

select *
from film f
         right join inventory i on f.film_id = i.film_id
where i.film_id is null
order by title

