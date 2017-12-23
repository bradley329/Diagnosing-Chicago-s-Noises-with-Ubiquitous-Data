create external table complaints_for_ml (id string, latitude double, longitude double, one  string, two  string,
three  string, four  string, five  string, six  string, seven  string, eight  string, nine  string, ten  string,
eleven  string, twelve  string, thirteen  string, fourteen  string, fifteen  string, sixteen string, num_complaints int)
row format delimited fields terminated by '\t'
location '/user/lg2681/12_6/complaints_for_ml/';

create table complaints_for_ml_final as select id, num_complaints from complaints_for_ml
WHERE (num_complaints > 2);

select * from complaints_for_ml_final;

create table complaints_for_inference as select latitude, longitude, num_complaints from complaints_for_ml
WHERE (num_complaints > 2);
select * from complaints_for_inference;

create external table traffic_for_ml_final (id string, num_traffic int)
row format delimited fields terminated by '\t'
location '/user/lg2681/12_6/traffic_for_ml/';

create external table checkins_for_ml_final (id string, num_checkins int)
row format delimited fields terminated by '\t'
location '/user/lg2681/12_6/checkins_for_ml/';

create external table pois_for_ml (id string, latitude double, longitude double, num_pois int)
row format delimited fields terminated by '\t'
location '/user/lg2681/12_6/pois_for_ml/';

create table pois_for_ml_final as select id, num_pois from pois_for_ml;


create table com_traffic as 
SELECT complaints_for_ml_final.id, complaints_for_ml_final.num_complaints, traffic_for_ml_final.num_traffic
FROM complaints_for_ml_final 
JOIN traffic_for_ml_final ON (complaints_for_ml_final.id = traffic_for_ml_final.id);

select * from com_traffic;

create table com_traffic_checkins as 
SELECT com_traffic.id, com_traffic.num_complaints, com_traffic.num_traffic, checkins_for_ml_final.num_checkins
FROM com_traffic 
JOIN checkins_for_ml_final ON (com_traffic.id = checkins_for_ml_final.id);

create table final as 
SELECT com_traffic_checkins.id, com_traffic_checkins.num_complaints, com_traffic_checkins.num_traffic, 
com_traffic_checkins.num_checkins, pois_for_ml_final.num_pois
FROM com_traffic_checkins 
JOIN pois_for_ml_final ON (com_traffic_checkins.id = pois_for_ml_final.id);

create table final_final as select num_complaints, num_traffic, num_checkins, num_pois from final;



create table data_
/*FROM ((complaints_for_ml_final
JOIN traffic_for_ml_final ON (complaints_for_ml_final.id = traffic_for_ml_final.id))
JOIN checkins_for_ml_final ON (complaints_for_ml_final.id = checkins_for_ml_final.id))
JOIN pois_for_ml_final ON (complaints_for_ml_final.id = pois_for_ml_final.id);*/

create table traffic_checkins as 
SELECT traffic_for_ml_final.id, traffic_for_ml_final.num_traffic, checkins_for_ml_final.num_checkins
FROM traffic_for_ml_final
JOIN checkins_for_ml_final ON (traffic_for_ml_final.id = checkins_for_ml_final.id);

select * from traffic_checkins;

create table traffic_checkins_pois as 
SELECT traffic_checkins.id, traffic_checkins.num_traffic, traffic_checkins.num_checkins, pois_for_ml_final.num_pois
FROM traffic_checkins
JOIN pois_for_ml_final ON (traffic_checkins.id = pois_for_ml_final.id);

create table selected_noid as 
SELECT complaints_for_ml.num_complaints, traffic_checkins_pois.num_traffic, traffic_checkins_pois.num_checkins, traffic_checkins_pois.num_pois
FROM complaints_for_ml
RIGHT OUTER JOIN traffic_checkins_pois ON (complaints_for_ml.id = traffic_checkins_pois.id);

create table selected_withid as 
SELECT traffic_checkins_pois.id, complaints_for_ml.num_complaints, traffic_checkins_pois.num_traffic, traffic_checkins_pois.num_checkins, traffic_checkins_pois.num_pois
FROM complaints_for_ml
RIGHT JOIN traffic_checkins_pois ON (complaints_for_ml_final.id = traffic_checkins_pois.id);