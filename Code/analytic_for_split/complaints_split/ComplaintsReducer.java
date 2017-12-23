import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ComplaintsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        		String val = "";
        		String comment = "";
        		 String type = "";
                for (Text value : values) { 
                        val = value.toString();
                        String[] p = val.split("\t");
                        comment = p[3];                       
                        if(comment.contains("MUSIC")||comment.contains("PARTY")){
                        	type = "MUSIC/PARTY";
                        }
                        else if(comment.contains("A/C")||comment.contains("VENTILATION")){
                        	type = "AC/VENTILATION";
                        }
                        else if(comment.contains("BANG")||comment.contains("POUND")){
                        	type = "BANG/POUND";
                        }
                        else if(comment.contains("VEHICLE")
                        		||comment.contains("CAR")
                        		||comment.contains("TRUCK")
                        		||comment.contains("TRAIN")
                        		||comment.contains("VENT")){
                        	type = "TRAFFIC";
                        }
                        else if(comment.contains("CONSTRUCT")){
                        	type = "CONSTRUCTION";
                        }
                        else if(comment.contains("EQUIPMENT")||comment.contains("REPAIR")){
                        	type = "REPAIR/REFURBISH";
                        }
                        else if(comment.contains("HAMMER")||comment.contains("JACK")){
                        	type = "HAMMER/JACK";
                        }
                        else {
                        	type ="OTHERS";
                        }
                        String output = p[0] + "\t" + p[1] + "\t" + p[2] + "\t"+ type;
                        context.write(key, new Text(output));
                }
               
                 }                      
 }

