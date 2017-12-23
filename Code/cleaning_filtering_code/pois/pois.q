create external table poi (id string, latitude double, longitude double, location1 string, location2 string)
          row format delimited fields terminated by '\t'
          location '/user/lg2681/pois/';
          --select id, latitude, longitude from poi;
          select id, latitude, longitude 
          into poic from poi;
create table poic as select id, latitude, longitude from poi;
