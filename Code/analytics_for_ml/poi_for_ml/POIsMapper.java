import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class POIsMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		String loc = p[0];
		String count = p[3]; 
		context.write(new Text(loc), new Text(count));
		}
}
	



