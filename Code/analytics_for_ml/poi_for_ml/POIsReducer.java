import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class POIsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		int count = 0;
        		String val = "";
                for (Text value : values) {
                        count++;   
                        val = value.toString();	
                }
                String output = val + "\t" + count;
                context.write(key, new Text(output));
                 }
                      
 }