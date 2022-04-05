-- Creating the test table
CREATE TABLE job_salary(
   EMPID        INTEGER  NOT NULL PRIMARY KEY
  ,NAME         VARCHAR(9) NOT NULL
  ,JOB          VARCHAR(14) NOT NULL
  ,SALARY       INTEGER  NOT NULL
  ,total_salary INTEGER  NOT NULL
);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (201,'ANIRUDDHA','ANALYST',2100,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (212,'LAKSHAY','DATA ENGINEER',2700,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (209,'SIDDHARTH','DATA ENGINEER',3000,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (232,'ABHIRAJ','DATA SCIENTIST',3000,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (205,'RAM','ANALYST',2500,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (222,'PRANAV','MANAGER',4500,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (202,'SUNIL','MANAGER',4800,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (233,'ABHISHEK','DATA SCIENTIST',2800,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (244,'PURVA','ANALYST',2500,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (217,'SHAROON','DATA SCIENTIST',3000,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (216,'PULKIT','DATA SCIENTIST',3500,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (200,'KUNAL','MANAGER',5000,42200);
INSERT INTO job_salary(EMPID,NAME,JOB,SALARY,total_salary) VALUES (210,'SHIPRA','ANALYST',2800,42200);


DROP TABLE IF EXISTS sales;
CREATE TABLE sales(
   year     INTEGER  NOT NULL
  ,group_id INTEGER  NOT NULL
  ,amount   NUMERIC(7,2) NOT NULL
);
INSERT INTO sales(year,group_id,amount) VALUES (2018,1,1474.00);
INSERT INTO sales(year,group_id,amount) VALUES (2018,2,1787.00);
INSERT INTO sales(year,group_id,amount) VALUES (2018,3,1760.00);
INSERT INTO sales(year,group_id,amount) VALUES (2019,1,1915.00);
INSERT INTO sales(year,group_id,amount) VALUES (2019,2,1911.00);
INSERT INTO sales(year,group_id,amount) VALUES (2019,3,1118.00);
INSERT INTO sales(year,group_id,amount) VALUES (2020,1,1646.00);
INSERT INTO sales(year,group_id,amount) VALUES (2020,2,1975.00);
INSERT INTO sales(year,group_id,amount) VALUES (2020,3,1516.00);

CREATE TABLE sales_stats(
    name VARCHAR(100) NOT NULL,
    year SMALLINT NOT NULL CHECK (year > 0),
    amount DECIMAL(10,2) CHECK (amount >= 0),
    PRIMARY KEY (name,year)
);

INSERT INTO
    sales_stats(name, year, amount)
VALUES
    ('John Doe',2018,120000),
    ('Jane Doe',2018,110000),
    ('Jack Daniel',2018,150000),
    ('Yin Yang',2018,30000),
    ('Stephane Heady',2018,200000),
    ('John Doe',2019,150000),
    ('Jane Doe',2019,130000),
    ('Jack Daniel',2019,180000),
    ('Yin Yang',2019,25000),
    ('Stephane Heady',2019,270000);
--  Window function with agrigation funtion
select empid, name, job, salary, total_salary,
	sum(salary) over(PARTITION by job)
from public.job_salary;

select empid, name, job, salary, total_salary,
	sum(salary) over (PARTITION by job ORDER BY name desc) as job_salary
from public.job_salary;

--  1. CUME_DIST function with partitioning
select empid, name, job, salary, total_salary,
	 cume_dist() over (PARTITION by job ORDER BY salary desc) as job_salary
from public.job_salary;

--  2. Dense function with partitioning
with sum_salary as(
select empid, name, job, salary, total_salary,
	sum(salary) over (PARTITION by job ORDER BY salary desc) as job_salary
from public.job_salary)
select empid, name, job, salary, total_salary, job_salary,
 DENSE_Rank() over(PARTITION by job ORDER BY job_salary)
from sum_salary;

-- 3. FIRST_VALUE()
with sum_salary as(
select empid, name, job, salary, total_salary,
	sum(salary) over (PARTITION by job ORDER BY salary desc) as job_salary
from public.job_salary)
select empid, name, job, salary, total_salary, job_salary,
 FIRST_VALUE(salary) over(PARTITION by job ORDER BY job_salary)
from sum_salary;

-- 4 LAG() Function
with sum_salary as(
select job,
	sum(salary) as job_salary
from public.job_salary
    group by job
    order by job
),
sum_salary_lag as(
select job, job_salary,
    LAG(job_salary, 1 ) over (ORDER BY job desc) as job_salary_lag
from sum_salary
    )
select job, job_salary, job_salary_lag
from sum_salary_lag;

with year_total as(
    select year,
           sum(amount) as amount
    from sales
    group by year
    order by year),
    per_year_total as(
        select year,
               amount,
               lag(amount, 1) over (order by year) as previouse_year_total
        from year_total
    )
select year, amount, previouse_year_total
from per_year_total;

-- Last_Value() function
with last_salary as(
    select empid,
           name,
            job,
           salary,
           total_salary,
           last_value(salary) over (
               partition by job
               order by salary
               RANGE BETWEEN
               UNBOUNDED PRECEDING AND
               UNBOUNDED FOLLOWING) as highest_salary
    from job_salary
    )
select empid,
       name,
       job,
       salary,
       highest_salary,
       (highest_salary-salary) as salary_diff,
       total_salary
from last_salary;

-- Lead() Function
with cet1 as (
    select year,
           sum(amount) as year_sales
    from sales
    group by year
    order by year
), cet2 as (
    select year,
           year_sales,
           lead(year_sales, 1) over (order by year) as next_year_sales
    from cet1
)
select year,
       year_sales,
       next_year_sales,
       (next_year_sales - year_sales) as sales_diffrence
from cet2;

-- NTILE()
select name,
       amount,
       year,
       ntile(2) over (order by amount)
from sales_stats;

select name,
       amount,
       year,
       ntile(3) over (partition by year order by year)
from sales_stats;

-- NTH_VALUE()
select empid,
       name,
       job,
       salary,
       total_salary,
       nth_value(job, 6) over (order by job)
from job_salary

select empid,
       name,
       job,
       salary,
       total_salary,
       nth_value(job, 2) over (partition by job order by job)
from job_salary

-- Rank() function
select empid,
       name,
       job,
       salary,
       total_salary,
       rank() over(partition by job order by job)
from job_salary

select empid,
       name,
       job,
       salary,
       total_salary,
       rank() over( order by salary)
from job_salary




