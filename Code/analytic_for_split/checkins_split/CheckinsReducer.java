import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class CheckinsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		int count = 0;
        		String[] id = key.toString().split(" ");
        		int i = Integer.parseInt(id[0]);
        		int j = Integer.parseInt(id[1]);
        		String time = id[2];
                for (Text value : values) {
                        		count ++;
                }
                double latitude = 41.637916+0.02*(2*i+1)/2;
 				double longitude = -87.862653+0.02*(2*j+1)/2;
                String k = Integer.toString(i) + "\t" + Integer.toString(j);
                String res = time + "\t" + Integer.toString(count) + "\t" + Double.toString(latitude) + "\t" + Double.toString(longitude);
                context.write(new Text(k), new Text(res));
                //String result = Integer.toString(count);
               // context.write(key, new Text(result));
        }
}
