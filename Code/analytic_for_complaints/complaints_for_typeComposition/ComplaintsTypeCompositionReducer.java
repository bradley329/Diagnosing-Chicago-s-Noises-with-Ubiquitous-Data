import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ComplaintsTypeCompositionReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		int musicCount = 0, acCount = 0, bangCount = 0, vehicleCount = 0,
        				constructCount = 0, equipCount = 0, hammerCount = 0, otherCount = 0,
        				totalCount = 0;
        		float musicPercent = 0, acPercent = 0, bangPercent = 0, vehiclePercent = 0,
        				constructPercent = 0, equipPercent = 0, hammerPercent = 0, 
        				otherPercent = 0;
        		String latitude = "";
        		String longitude = "";
                for (Text value : values) { 
                        String val = value.toString();
                        String[] p = val.split("\t");
                        latitude = p[0];
                        longitude = p[1];
                        String type = p[2];                       
                        if(type.equals("MUSIC/PARTY")){
                        	musicCount++;
                        }
                        else if(type.equals("AC/VENTILATION")){
                        	acCount++;
                        }
                        else if(type.equals("BANG/POUND")){
                        	bangCount++;
                        }
                        else if(type.equals("TRAFFIC")){
                        	vehicleCount++;
                        }
                        else if(type.equals("CONSTRUCTION")){
                        	constructCount++;
                        }
                        else if(type.equals("REPAIR/REFURBISH")){
                        	equipCount++;
                        }
                        else if(type.equals("HAMMER/JACK")){
                        	hammerCount++;
                        }
                        else {
                        	otherCount++;
                        }
                        totalCount++;
                }
                DecimalFormat form = new DecimalFormat("0.00");
                musicPercent = musicCount* 100f / totalCount;
                acPercent = acCount * 100f / totalCount;
                bangPercent = bangCount * 100f / totalCount;
                vehiclePercent = vehicleCount * 100f / totalCount;
                constructPercent = constructCount * 100f / totalCount;
                equipPercent = equipCount * 100f / totalCount;
                hammerPercent = hammerCount * 100f / totalCount;
                otherPercent = otherCount * 100f / totalCount;
                
                String output = latitude + "\t" + longitude + 
                		"\t" + Integer.toString(musicCount) +
                		"\t" + form.format(musicPercent) +
                		"\t" + Integer.toString(acCount) +
                		"\t" + form.format(acPercent) +
                		"\t" + Integer.toString(bangCount) +
                		"\t" + form.format(bangPercent) +
                		"\t" + Integer.toString(vehicleCount) +
                		"\t" + form.format(vehiclePercent) +
                		"\t" + Integer.toString(constructCount) +
                		"\t" + form.format(constructPercent) +
                		"\t" + Integer.toString(equipCount) +
                		"\t" + form.format(equipPercent) +
                		"\t" + Integer.toString(hammerCount) +
                		"\t" + form.format(hammerPercent) +
                		"\t" + Integer.toString(otherCount) +
                		"\t" + form.format(otherPercent) +
                		"\t" + Integer.toString(totalCount);
                context.write(key, new Text(output));
        }                      
 }

