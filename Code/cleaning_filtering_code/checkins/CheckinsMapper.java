import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class CheckinsMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		String id = "result";
		String time = p[1];
		String latitude = p[2];
		String longitude = p[3];
		String res = time + " " + latitude + " " + longitude;
		context.write(new Text(id), new Text(res));
		}
}