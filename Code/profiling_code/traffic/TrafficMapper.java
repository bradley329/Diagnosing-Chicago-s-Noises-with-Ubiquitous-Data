import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class TrafficMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split(",");
		String time = p[0];
		String region_id = p[1];
		String reads = p[3];
		String res = region_id + " " + reads;
		String k = "123";
		context.write(new Text(k), new Text(res));
		}
}
	



