# 1 answer

update bajaj_auto
set `date` = str_to_date(`date`,'%d-%M-%Y');

drop table if exists bajaj1;
create table bajaj1 as
select `Date`,`Close price`,
avg(`close price`) over(order by `date` rows 19 preceding)   as   `20 day MA`,
avg(`close price`) over(order by `date` rows 49 preceding)   as   `50 day MA`
from bajaj_auto;


update eicher_motors
set `date` = str_to_date(`date`,'%d-%M-%Y');

drop table if exists eicher1;
create table eicher1 as
select `Date`,`Close price`,
avg(`close price`) over(order by `date` rows 19 preceding)   as   `20 day MA`,
avg(`close price`) over(order by `date` rows 49 preceding)   as   `50 day MA`
from eicher_motors;


update hero_motorcorp
set `date` = str_to_date(`date`,'%d-%M-%Y');

drop table if exists hero1;
create table hero1 as
select `Date`,`Close price`,
avg(`close price`) over(order by `date` rows 19 preceding)   as   `20 day MA`,
avg(`close price`) over(order by `date` rows 49 preceding)   as   `50 day MA`
from hero_motorcorp;


update infosys
set `date` = str_to_date(`date`,'%d-%M-%Y');

drop table if exists infosys1;
create table infosys1 as
select `Date`,`Close price`,
avg(`close price`) over(order by `date` rows 19 preceding)   as   `20 day MA`,
avg(`close price`) over(order by `date` rows 49 preceding)   as   `50 day MA`
from infosys;


update tcs
set `date` = str_to_date(`date`,'%d-%M-%Y');

drop table if exists tcs1;
create table tcs1 as
select `Date`,`Close price`,
avg(`close price`) over(order by `date` rows 19 preceding)   as   `20 day MA`,
avg(`close price`) over(order by `date` rows 49 preceding)   as   `50 day MA`
from tcs;


update tvs_motors
set `date` = str_to_date(`date`,'%d-%M-%Y');

drop table if exists tvs1;
create table tvs1 as
select `Date`,`Close price`,
avg(`close price`) over(order by `date` rows 19 preceding)   as   `20 day MA`,
avg(`close price`) over(order by `date` rows 49 preceding)   as   `50 day MA`
from tvs_motors;

# 2 answer

drop table if exists master_table;
create table master_table as
select b.`date`, b.`close price` as bajaj,t.`close price` as tcs,v.`close price` as tvs,i.`date` as infosys, e.`close price` as eicher, h.`Close price` as hero
from bajaj1 b
left join tcs1    t on b.`date` = t.`date`
left join tvs1    v on b.`date` = v.`date`
left join infosys i on b.`date` = i.`date`
left join eicher1 e on b.`date` = e.`date`
left join hero1   h on b.`date` = h.`date` 
order by b.`date`;

# 3 answer

drop function if exists signal_gen;
DELIMITER $$
 
CREATE FUNCTION signal_gen(`20 day MA` double , `50 day MA` double) RETURNS VARCHAR(10)
    DETERMINISTIC
BEGIN
    DECLARE `signal` varchar(4);
 
    IF  `20 day MA` > `50 day MA` THEN
 SET `signal` = 'buy';
    ELSEif `20 day MA` < `50 day MA` then 
    set  `signal` = 'sell' ;
    end if;
    
 RETURN (`signal`);
 END
$$

DELIMITER ;

# signal table for bajaj_auto
drop table if exists temp;
create table temp as
select `date`,`close price`,signal_gen(`20 day MA`,`50 day MA`) as `signal`
from bajaj1;

drop table if exists bajaj2;
create table bajaj2 as
SELECT
         `date`, `close price`,`signal`,
         lag(`signal`)  OVER w AS 'lag_signal'
       FROM temp
       WINDOW w AS (ORDER BY `date`
                    ROWS UNBOUNDED PRECEDING);

update bajaj2
set `signal` = 'hold' where (`lag_signal` = `signal`);

alter table bajaj2
drop column lag_signal;
 

# signal table for TCS

drop table if exists temp;
create table temp as
select `date`,`close price`,signal_gen(`20 day MA`,`50 day MA`) as `signal`
from tcs1;

drop table if exists tcs2;
create table tcs2 as
SELECT
         `date`, `close price`,`signal`,
         lag(`signal`)  OVER w AS 'lag_signal'
       FROM temp
       WINDOW w AS (ORDER BY `date`
                    ROWS UNBOUNDED PRECEDING);

update tcs2
set `signal` = 'hold' where (`lag_signal` = `signal`);

alter table tcs2
drop column lag_signal;


# signal table for TVS
drop table if exists temp;
create table temp as
select `date`,`close price`,signal_gen(`20 day MA`,`50 day MA`) as `signal`
from tvs1;

drop table if exists tvs2;
create table tvs2 as
SELECT
         `date`, `close price`,`signal`,
         lag(`signal`)  OVER w AS 'lag_signal'
       FROM temp
       WINDOW w AS (ORDER BY `date`
                    ROWS UNBOUNDED PRECEDING);

update tvs2
set `signal` = 'hold' where (`lag_signal` = `signal`);

alter table tvs2
drop column lag_signal;


# signal table for infosys
drop table if exists temp;
create table temp as
select `date`,`close price`,signal_gen(`20 day MA`,`50 day MA`) as `signal`
from infosys1;

drop table if exists infosys2;
create table infosys2 as
SELECT
         `date`, `close price`,`signal`,
         lag(`signal`)  OVER w AS 'lag_signal'
       FROM temp
       WINDOW w AS (ORDER BY `date`
                    ROWS UNBOUNDED PRECEDING);

update infosys2
set `signal` = 'hold' where (`lag_signal` = `signal`);

alter table infosys2
drop column lag_signal;


# signal table for eicher
drop table if exists temp;
create table temp as
select `date`,`close price`,signal_gen(`20 day MA`,`50 day MA`) as `signal`
from eicher1;

drop table if exists eicher2;
create table eicher2 as
SELECT
         `date`, `close price`,`signal`,
         lag(`signal`)  OVER w AS 'lag_signal'
       FROM temp
       WINDOW w AS (ORDER BY `date`
                    ROWS UNBOUNDED PRECEDING);

update eicher2
set `signal` = 'hold' where (`lag_signal` = `signal`);

alter table eicher2
drop column lag_signal;


# signal table for hero

drop table if exists temp;
create table temp as
select `date`,`close price`,signal_gen(`20 day MA`,`50 day MA`) as `signal`
from hero1;

drop table if exists hero2;
create table hero2 as
SELECT
         `date`, `close price`,`signal`,
         lag(`signal`)  OVER w AS 'lag_signal'
       FROM temp
       WINDOW w AS (ORDER BY `date`
                    ROWS UNBOUNDED PRECEDING);

update hero2
set `signal` = 'hold' where (`lag_signal` = `signal`);

alter table hero2
drop column lag_signal;
drop table if exists temp;

# 4 answer

drop function if exists bajaj_signal_generator;
DELIMITER $$
create function bajaj_signal_generator (`input` text)
returns varchar(4)
deterministic
begin
declare sign varchar(4) ;

 select `signal` into sign
 from bajaj2 where `date` = `input`;

return (sign);

end
$$

DELIMITER ;

