import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class InferenceMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		String community = p[0];
		String[] cc = p[0].split(",");
		int x = Integer.parseInt(cc[0]);
		int y = Integer.parseInt(cc[1]);;
		double lat = 41.637916+0.02*(2*x+1)/2;
		double logi = -87.862653+0.02*(2*y+1)/2;

		if(p[1]=="\\N"){
			int traffic = Integer.parseInt(p[2]);
			int checkin = Integer.parseInt(p[3]);
			int poi = Integer.parseInt(p[4]);
			double comp = 1.0000438720960052 + 0.029077866619915703 * traffic
					           + 0.03276810368217794 * checkin 
					           + 0.02068828881826676 * poi;
			String complaint = Double.toString(comp);
			String loc = Double.toString(lat) + "\t" + Double.toString(logi);
			String res = complaint; 
			context.write(new Text(loc), new Text(res));
		}     	
	}
}
