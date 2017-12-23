import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class ComplaintsTypeCompositionMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		String community = p[0];
		double latitude = Double.parseDouble(p[1]);
		double longitude = Double.parseDouble(p[2]);
		String type = p[4];
		
		String output = Double.toString(latitude) + "\t" + Double.toString(longitude)
		+ "\t" + type;	
		context.write(new Text(community), new Text(output));
		}
	}

	



