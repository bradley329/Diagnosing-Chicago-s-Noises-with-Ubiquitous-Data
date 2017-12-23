import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class CheckinsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		int count = 0;
                for (Text value : values) {
                        String[] input = value.toString().split(" ");
                        String time = input[0];
                        double latitude = Double.parseDouble(input[1]);
                        double  longitude = Double.parseDouble(input[2]);
                        if (41.637916 <= latitude && latitude <= 42.023684
                                        && -87.862653 <= longitude && longitude <= -87.522077) {
                                String lgi = Double.toString(longitude);
                                String lat = Double.toString(latitude);
                                String res = time + " " + lat + " " + lgi;
                                context.write(key, new Text(res));
                        }
                }
                //String result = Integer.toString(count);
               // context.write(key, new Text(result));
        }
}
