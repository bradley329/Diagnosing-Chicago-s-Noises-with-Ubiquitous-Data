import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ComplaintsWeekReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	
        		int weekDayCount = 0;
        		int weekEndCount = 0;

        		float weekDayCountAver = 0;
        		float weekEndCountAver = 0;
        		
        		String latitude = "";
        		String longitude = "";

                for (Text value : values) {
                	String val = value.toString();
                    String[] p = val.split("\t");
                    latitude = p[0];
                    longitude = p[1];
                    String weekTypeStr = p[2];
                    int weekType = Integer.parseInt(weekTypeStr);
                    
                    if(weekType == 0) {
                    	weekDayCount++;
                    }
                    else {
                    	weekEndCount++;
                    }
                }
                
                weekDayCountAver = (float) (weekDayCount / 5);
                weekEndCountAver = (float) (weekEndCount / 2);
                
                DecimalFormat form = new DecimalFormat("0.00");
                
                String output = latitude + "\t" + longitude + "\t" 
                + form.format(weekDayCountAver) + "\t"
                + Integer.toString(weekDayCount) + "\t"
                + form.format(weekEndCountAver) + "\t"
                + Integer.toString(weekEndCount);
                
                context.write(key, new Text(output));
        }                      
 }

