import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;



public class POIsReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		int maxleng = 0;
		int minleng = Integer.MAX_VALUE;
		double maxlat = 0.00000;
		double minlat = 180.00000;
		double maxlong = -180.00000;
		double minlong = -0.00000;
		for (Text value : values) {
			String[] input = value.toString().split("\t");
			String id = input[0];
			int len = id.length();
			double latitude = Double.parseDouble(input[1]);
			double  longitude = Double.parseDouble(input[2]);
			if (maxlat < latitude) {
				maxlat = latitude;			
			}
			if (minlat > latitude) {
				minlat = latitude;			
			}
			if (maxlong < longitude){
				maxlong = longitude;
			}
			if (minlong > longitude){
				minlong = longitude;
			}
			if (maxleng < len){
				maxleng = len;
			}
			if (minleng > len){
				minleng = len;
			}			
		}
		
		String res = "maxID = " + Integer.toString(maxleng) + " minID = " + Integer.toString(minleng) 
		+ " maxLatitude = " + Double.toString(maxlat) + " minLatitude = " + Double.toString(minlat) 
		+ " maxLongitude = " + Double.toString(maxlong) + " minLongitude = " + Double.toString(minlong);
		String k = "Result:";
		context.write(new Text(k), new Text(res));
	}
}

