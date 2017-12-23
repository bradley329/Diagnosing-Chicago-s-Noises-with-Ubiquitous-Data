import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class ComplaintsMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		if (p.length>=5){
		double latitude = Double.parseDouble(p[1]);
		double longitude = Double.parseDouble(p[2]);
		String date = p[3];
		String comment = p[4];
		String x = "", y = "";
		double lat = 0.0;
		double logi = 0.0;
		for (int i = 0; i <= 19; i++) {
			if (latitude >= 41.637916+0.02*i  && latitude <= 41.637916+0.02*(i+1)) {
				x = Integer.toString(i);
				lat = 41.637916+0.02*(2*i+1)/2;
			}
		}
		for (int j = 0; j <= 17; j++) {
			if (longitude >= -87.862653+0.02*j  && longitude <= -87.862653+0.02*(j+1)) {
				y = Integer.toString(j);
				logi = longitude = -87.862653+0.02*(2*j+1)/2;
			}
		}
		BigDecimal latDeci = new BigDecimal(lat);
		latDeci = latDeci.setScale(6,BigDecimal.ROUND_HALF_DOWN);
		BigDecimal logiDeci = new BigDecimal(logi);
		logiDeci = logiDeci.setScale(6,BigDecimal.ROUND_HALF_DOWN);
		
		String output = latDeci.toString() + "\t" + logiDeci.toString()
		+ "\t" + date + "\t" + comment;
		String loc = x + "," + y;		
		context.write(new Text(loc), new Text(output));
		}
		}
}
	



