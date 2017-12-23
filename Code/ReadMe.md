## Dataset Description:
	(1)
    	Name: CDPH_Environmental_Complaints.csv 
	Description: Environmental complaints data from CDPH as our main source of noise complaints data. This will serve as our main datasource for 	examining the noise level. 
	Link: https://data.cityofchicago.org/Environment-Sustainable-Development/CDPH-Environmental-Complaints/fypr-ksnz
	(2)
	Name: Chicago_Traffic_Tracker_-_Historical_Congestion_Estimates_by_Region.csv, Chicago_Traffic_Tracker_-_Congestion_Estimates_By_Regions.csv
	Description: the first dataset serves as the data source for traffic counts associated with region ids; the second dataset contains 29 different 	region ids and their corresponding locations(latitude and longitude).
	Links: https://data.cityofchicago.org/Transportation/Chicago-Traffic-Tracker-Historical-Congestion-Esti/emtn-qqdi
		  https://data.cityofchicago.org/Transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Re/t2qc-9pjd
	(3)
	Name: Gowalla_totalCheckins.txt
	Description: the dataset has a total of 6,442,890 check-ins over the period of Feb. 2009 - Oct. 2010.
	Link: https://snap.stanford.edu/data/loc-gowalla.html
	(4)
	Name: dataset_TIST2015_POIs.txt
	Description: the dataset contains all venue data with 7 columns, which are:Venue ID (Foursquare); Latitude; Longitude; Venue category name 	(Foursquare); Country code (ISO 3166-1 alpha-2 two-letter country codes).
	Link: https://drive.google.com/file/d/0BwrgZ-IdrTotZ0U0ZER2ejI3VVk/view

## Profiling data 
	In this process, we mainly used MapReduce. 
	First, we filtered and extracted Chicago's data from each dataset roughly according to it's range of latitude and longitude; Then we profiled the 	filtered datasets, and derived expected results in each field in the four datasets. Please check the codes and screenshots in folder "profiling_code" 	for details; 

## Cleaning and filtering data (ETL)
	In this process, we mainly used Hive to clean three datasets: POIs, Complaints and Traffic. As for Checkins, we used MapReduce. 
	Codes for cleaning and filtering data are in folder "cleaning_filtering_code";

## Analytics
	(1) In the first step, we came up with an idea similar to Finite Element Analysis, that is we partitioned Chicago into roughly 360 small areas with a 	stepsize of 0.02 in both latitude and longitude. Each area has a unique key as pairs (i,j) and each data which lies in a specific as key. We mainly 	used MapReduce in this step. 
	Codes for partitioning are in each Mapper tasks in the folder "analytic_split_code";

	(2) In this step, we labeled the type of each noise complaint in our complaints dataset according to key words;
	also we count the total pois and checkins in each area.
	Codes for this step are in Reducer tasks in the folder "analytic_split_code";

	(3) In this step, with the results from step (2), we calculated the total complaints count for each type of each area; also we calculated the average 	complaints count for each area during weekdays and weekends.
	Codes for this step are in the folder "analytic_complalints_code";

	(4) In this step, since the complaints for some areas are missing and some complaint counts are unauthentic, we decided to do Linear Regression among 	complaints count, traffic count, pois count and checkins count in order to find their correlationship through Spark using MLlib. So first, we used 	MapReduce and Hive together to prepare the data.
	Codes for this step are in the folder "analytics_for_ml".

	(5) In this step, we used the scala-shell in Spark by using "MLlib" for our Linear Regression among complaints count, traffic count, pois count and 	checkins count. We mainly used the LinearRegressionWithSGD model and fixed step size is 0.000001 (very important!).
	Codes for this step and the model we got are in the folder "analytics_for_regression".

	(6) Finally, we made use of the regression model in step (5) to infer the missing complaints data in some areas where traffic, pois and checkins are 	all available. After that we got an extra inference result for over 20 areas. 
	Codes for this step and the inference result we got are in the folder "analytics_for_inference".