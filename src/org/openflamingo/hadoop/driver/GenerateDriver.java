package org.openflamingo.hadoop.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openflamingo.hadoop.generate.CounterMapper;
import org.openflamingo.hadoop.generate.GenerateMapper;

/**
 * <code>GenerateDriver</code> manages and runs only generate ETL.
 * @author Hyunje
 */
public class GenerateDriver extends Configured implements Tool {
	/**
	 * Runs using <code>ToolRunner</code>
	 * @param args arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new GenerateDriver(), args);
		System.exit(res);
	}

	/**
	 * Run the job.
	 * <p>Generate has two jobs. First job is <code>countJob</code>, second one is <code>generateJob</code>.
	 * <pre><code>countJob</code> save the number of lines for each mapper.</pre>
	 * <pre><code>generateJob</code> generates unique number of each row.</pre>
	 *
	 * @param args arguments
	 * @return exit status of job.
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job countJob = new Job();
		Job generateJob = new Job();

		//Parse Arguments and Setting Job.
		parseArguementsAndSetCountJob(args, countJob, generateJob);
		//Run counter job.
		countJob.waitForCompletion(true);
		Counters counters = countJob.getCounters();
		//Check status of finished value of counter job.
		//Set index parameter for generate Mapper
		CounterGroup localCounterGroup = counters.getGroup("LocalCounter".toUpperCase());
		CounterGroup globalCounterGroup = counters.getGroup("globalCounter".toUpperCase());
		Long globalCountSize = globalCounterGroup.findCounter("counter".toUpperCase()).getValue();
		int startIndex = 0;
		for (Counter each : localCounterGroup) {
			String mapperID = each.getName();
			Long mapperCountSize = each.getValue();
			generateJob.getConfiguration().set(mapperID, startIndex + "," + mapperCountSize.toString());
			startIndex = startIndex + Integer.parseInt(mapperCountSize.toString());
		}
		// Run a Generate Job
		return generateJob.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Parse arguments and set count job and generate job using parsed result.
	 * @param args arguments
	 * @param countJob countJob
	 * @param genJob generateJob
	 * @throws Exception
	 */
	private void parseArguementsAndSetCountJob(String[] args, Job countJob, Job genJob) throws Exception {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("-input")) {
				FileInputFormat.addInputPaths(countJob, args[++i]);
				FileInputFormat.addInputPaths(genJob, args[i]);
			} else if (args[i].equals("-countoutput")) {
				FileOutputFormat.setOutputPath(countJob, new Path(args[++i]));
			} else if (args[i].equals("-generateoutput")) {
				FileOutputFormat.setOutputPath(genJob, new Path(args[++i]));
			} else if (args[i].equals("-jobName")) {
				countJob.getConfiguration().set("mapred.job.name", args[++i]);
				genJob.getConfiguration().set("mapred.job.name", args[i]);
			} else if (args[i].equals("-indelimiter")) {
				countJob.getConfiguration().set("indelimiter", args[++i]);
				genJob.getConfiguration().set("indelimiter", args[i]);
			} else if (args[i].equals("-outdelimiter")) {
				countJob.getConfiguration().set("outdelimiter", args[++i]);
				genJob.getConfiguration().set("outdelimiter", args[i]);
			}

			//Set Count Job
			countJob.setMapperClass(CounterMapper.class);
			countJob.setMapOutputKeyClass(NullWritable.class);
			countJob.setMapOutputValueClass(NullWritable.class);
			countJob.setNumReduceTasks(0);
			countJob.setJarByClass(GenerateDriver.class);

			//Set generate Job
			genJob.setMapperClass(GenerateMapper.class);
			genJob.setMapOutputKeyClass(NullWritable.class);
			genJob.setMapOutputValueClass(Text.class);
			genJob.setNumReduceTasks(0);
			genJob.setJarByClass(GenerateDriver.class);


		}
	}
}
