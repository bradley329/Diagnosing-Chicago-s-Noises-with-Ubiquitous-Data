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
		String x = p[0];
		String y = p[1];
		String loc = x + "," + y;
		String count = p[3];
		context.write(new Text(loc), new Text(count));
     	
	}
}