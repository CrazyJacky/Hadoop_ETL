package org.openflamingo.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Hadoop MapReduce Sample Driver.
 */
public class SampleDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new SampleDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job();
		parseArguements(args, job);

		job.setJarByClass(SampleDriver.class);

		// Mapper Class
		job.setMapperClass(SampleMapper.class);

		// Output Key/Value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer Task
		job.setNumReduceTasks(0);

		// Run a Hadoop Job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void parseArguements(String[] args, Job job) throws IOException {
		for (int i = 0; i < args.length; ++i) {
			if ("-input".equals(args[i])) {
				FileInputFormat.addInputPaths(job, args[++i]);
			} else if ("-output".equals(args[i])) {
				FileOutputFormat.setOutputPath(job, new Path(args[++i]));
			} else if ("-jobName".equals(args[i])) {
				job.getConfiguration().set("mapred.job.name", args[++i]);
			} else if ("-columnToClean".equals(args[i])) {
				job.getConfiguration().set("columnToClean", args[++i]);
			} else if ("-delimiter".equals(args[i])) {
				job.getConfiguration().set("delimiter", args[++i]);
			}
		}
	}
}