import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class TrafficReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
                for (Text value : values) {
                	context.write(key, value);
                }
                //String result = Integer.toString(count);
               // context.write(key, new Text(result));
        }
}
