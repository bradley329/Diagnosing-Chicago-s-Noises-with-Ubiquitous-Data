create database <zy948> ;
use <zy948> ;
create external table w1(time string, region_id int, bus_count int, number_of_reads int, speed double, id string)
 		row format delimited fields terminated by ','
		location '/user/zy948/hivetraffic/';
show tables;
describe w1;
select * from w1 limit 2;
create table traffic as select time, region_id, number_of_reads from w1;
select * from traffic limit 2;
