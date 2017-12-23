import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class ComplaintsMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String ident = "-87";
        String noise = "Noise";
        if (line.indexOf(ident) != -1 && line.indexOf(noise) != -1) {
            String[] p = line.split(",");
              if (p.length > 3) {
            	 String tp = p[1]; //Column B
            	 String date = p[11]; //Column J            	 
                 String content = "";
                 for (int index = 12; index < p.length - 3; index++) {
                     content = content + p[index];
                 } //Column K

                 String rawLatt = p[3];  //e.g.  IL (41.777285
                 String rawLongi = p[4];  //e.g. " "-87.743467)

                 String latt = rawLatt.substring(4);  //latitude in Column C
                 
                 String longi = rawLongi.substring(1, rawLongi.length()-2); //longi in Column C
                 String result = latt + "\t" + longi + "\t" + content + "\t" + date;
                 context.write(new Text(noise), new Text(result));
        
            }  
    }
}
}




