import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;



public class TrafficReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		
		int max_region_id = Integer.MIN_VALUE;
		int min_region_id = Integer.MAX_VALUE;
		
		int max_reads = Integer.MIN_VALUE;
		int min_reads = Integer.MAX_VALUE;
		
		for (Text value : values) {
			String[] input = value.toString().split(" ");
			int region_id = Integer.parseInt(input[0]);
			int reads = Integer.parseInt(input[1]);

			if (max_region_id < region_id) {
				max_region_id = region_id;			
			}
			if (min_region_id > region_id) {
				min_region_id = region_id;			
			}
			if (max_reads < reads){
				max_reads = reads;
			}
			if (min_reads > reads){
				min_reads = reads;
			}
		}
		String res = "type of column 1 is time; " + " range of column 2(region_id) = " + min_region_id 
				+ " to " + max_region_id + " range of column 3(reads) = " + min_reads + " to " + max_reads;
		String k = "Result: ";
		context.write(new Text(k), new Text(res));
	}
}
