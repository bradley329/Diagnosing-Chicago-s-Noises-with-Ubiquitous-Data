import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Complaints {

  public static void main(String[] args)  throws Exception {
	if (args.length !=2) {
		System.err.println("Usage: Str <input path> <output path>");
		System.exit(-1);
	}

	Job job = new Job();
	job.setJarByClass(Complaints.class);
	job.setJobName("Complaints");

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.setMapperClass(ComplaintsMapper.class);
	job.setReducerClass(ComplaintsReducer.class);
	job.setNumReduceTasks(1);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
  }



