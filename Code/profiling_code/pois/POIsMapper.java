import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class POIsMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		String id = p[0];
		String latstr = p[1];
		String longstr = p[2];
		double latitude = Double.parseDouble(p[1]);
        double  longitude = Double.parseDouble(p[2]);
		String k = "Profile";
        if (41.637916 <= latitude && latitude <= 42.023684
                        && -87.862653 <= longitude && longitude <= -87.522077) {
                String lgi = Double.toString(longitude);
                String lat = Double.toString(latitude);
                String res = lat + " " + lgi;
		String result = id + "\t" + latitude + "\t" + longitude;

		context.write(new Text(k), new Text(result));
		}
        }
}
	




