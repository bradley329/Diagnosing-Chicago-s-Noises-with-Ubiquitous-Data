import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class ComplaintsWeekMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] p = line.split("\t");
		
		String dat = p[3];
		
		try {

			SimpleDateFormat format1 = new SimpleDateFormat("MM/dd/yyyy");
			Date dt1 = format1.parse(dat);
			Calendar c = Calendar.getInstance();
			c.setTime(dt1);
			int wekDy = c.get(Calendar.DAY_OF_WEEK);
			
			int res = 0;
			if (res == 1 || res == 7) res = 1;
			else res = 0;
			
			String community = p[0];
			
			String output = p[1] + "\t" + p[2] + "\t" + Integer.toString(res);	
			context.write(new Text(community), new Text(output));
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
		
		}
	}

	



