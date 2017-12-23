import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class CheckinsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		int count = 0; 
        		for (Text value : values) {
        	    	String line = value.toString();
        	    	int rd = 0;
        	    	rd = Integer.parseInt(line);
                	count += rd;                                   	
                } 
        		String res = Integer.toString(count);
                context.write(key, new Text(res));             
        }
}
