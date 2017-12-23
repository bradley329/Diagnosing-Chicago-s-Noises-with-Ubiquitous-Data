import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class CheckinsMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t| ");
		String time = p[1].substring(0, 10);
		double latitude = Double.parseDouble(p[2]);
		double  longitude = Double.parseDouble(p[3]);
		for (int i=0; i<=19; i++){
     		for (int j=0; j<=17; j++){
     			if (latitude >= 41.637916+0.02*i  && latitude <= 41.637916+0.02*(i+1) && longitude >= -87.862653+0.02*j  && longitude <= -87.862653+0.02*(j+1)){
     				String k = Integer.toString(i) + " " + Integer.toString(j) + " " + time;
     				String res = latitude + " " + longitude;
     				context.write(new Text(k), new Text(res));
     		}
     	}
	}
}
}
