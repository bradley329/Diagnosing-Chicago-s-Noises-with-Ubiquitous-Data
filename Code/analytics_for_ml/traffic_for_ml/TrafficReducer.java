import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class TrafficReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		float count = 0; float reads = 0; int avr = 0;
                for (Text value : values) {
                	float rd = 0;
                	count++;
                	String line = value.toString();
                	rd = Integer.parseInt(line);
                	reads += rd;                	
                }
                avr = Math.round(reads/count);
                String average = Integer.toString(avr);
                context.write(key, new Text(average));
        }
}
