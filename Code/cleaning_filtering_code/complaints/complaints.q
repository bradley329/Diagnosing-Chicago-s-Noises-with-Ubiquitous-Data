create external table complaints (id string, type string, street string, latitude string, longitude string,
	d1 string, e1 string, f1 string, g1 string, h1 string, i1 string, date1 string, comment1 string, l1 string, m1 string, n1 string)
          row format delimited fields terminated by ','
          location '/user/lg2681/nocomplaints/';

describe complaints;
create table complaintsc as select type, latitude, longitude, date1, comment1 from complaints;
--select useful colomns
describe complaintsc;
select * from complaintsc;
create table complaint1 as select * from complaintsc where type='Noise Complaint';
--select rows where complaint type is noise
create table complaint2 as select  * from complaint1 where longitude like '%-87%';
--select rows which have longitude
select * from complaint2;
--select  from complaintsc where keyword like '%%' 
create table complaint3 as select type, substr(latitude, 5) as latitude,  substr(longitude,1, length(longitude)-2) as longitude,
date1, comment1 from complaint2;
select * from complaint3;
create table complainttemp as select * from complaint3 where date1 like '%201%';--select rows where year is "201x"
select * from complainttemp;
create table complainttemp1 as select * from complaint3 where date1 like '%200%';--select rows where year is "200x"
select * from complainttemp1;
insert into complainttemp select * from complainttemp1;
--insert table where year is "200x" into table where year is "201x"
create table complaintcc (type string, latitude double, longitude double, date1 string, comment1 string);
--convert latitude and longitude from string to double
insert into complaintcc select * from complainttemp; 