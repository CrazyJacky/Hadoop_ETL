package org.openflamingo.hadoop.driver;


import org.openflamingo.hadoop.aggregate.AggregateMapper;
import org.openflamingo.hadoop.clean.CleanMapper;
import org.openflamingo.hadoop.filter.FilterMapper;
import org.openflamingo.hadoop.grep.GrepMapper;
import org.openflamingo.hadoop.group.GroupMapper;
import org.openflamingo.hadoop.group.GroupReducer;
import org.openflamingo.hadoop.replace.ReplaceMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver for ETL.
 *
 * @author Hyunje
 * @since 2012-08
 */
public class MainDriver extends Configured implements Tool {
	Job job;
	Job generateJob;
	public static void main(String[] args)throws Exception{
		int res = ToolRunner.run(new MainDriver(),args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		job = new Job();
		generateJob = null;

		//Parse Arguments and Setting Job.
		parseArguementsAndSetJob(args);

		//Set Jar
		job.setJarByClass(MainDriver.class);

		// Run a Hadoop Job
		return job.waitForCompletion(true) ? 0 : 1;
	}
	private void parseArguementsAndSetJob(String[] args) throws Exception {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("-input")) {
				FileInputFormat.addInputPaths(job, args[++i]);
			} else if (args[i].equals("-output")) {
				FileOutputFormat.setOutputPath(job, new Path(args[++i]));
			} else if (args[i].equals("-jobName")) {
				job.getConfiguration().set("mapred.job.name", args[++i]);
			} else if (args[i].equals("-indelimiter")) {
				job.getConfiguration().set("indelimiter", args[++i]);
			} else if (args[i].equals("-outdelimiter")) {
				job.getConfiguration().set("outdelimiter", args[++i]);
			} else if (args[i].equals("-clean")){
				//Mapper Class
				job.setMapperClass(CleanMapper.class);

				//Output Key/Value
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);

				//Reducer Task
				job.setNumReduceTasks(0);

				//Set Parameter
				job.getConfiguration().set("target",args[++i]);
			} else if (args[i].equals("-aggregate")){
				//Mapper Class
				job.setMapperClass(AggregateMapper.class);

				//Output Key/Value
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);

				//Reducer Task
				job.setNumReduceTasks(0);

				//Set Parameter
				FileInputFormat.addInputPaths(job, args[++i]);
			} else if (args[i].equals("-replace")){
				//Mapper Class
				job.setMapperClass(ReplaceMapper.class);

				//Output Key/Value
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);

				//Reducer Task
				job.setNumReduceTasks(0);

				//Set Parameters
				job.getConfiguration().set("targetColumn",args[++i]);
				job.getConfiguration().set("oldValue",args[++i]);
				job.getConfiguration().set("newValue",args[++i]);
			} else if (args[i].equals("-filter")) {

				// Mapper Class
				job.setMapperClass(FilterMapper.class);

				// Output Key/Value
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);

				// Reducer Task
				job.setNumReduceTasks(0);

				//Set Parameters
				//value : EMPTY, NEMPTY, EQ, NEQ, GT, LT, GTE, LTE, START, END
				job.getConfiguration().set("targetColumn",args[++i]);

				if(args[i+1] != null){
					if(args[i + 1].equals("EMPTY") || args[i+1].equals("NEMPTY"))
					{
						job.getConfiguration().set("commandName", args[++i].toLowerCase());
						job.getConfiguration().set("value",null);
					}
					else
					{
						job.getConfiguration().set("commandName",args[++i].toLowerCase());
						job.getConfiguration().set("value",args[++i]);
					}
				}
			} else if (args[i].equals("-grep")){
				//Mapper Class
				job.setMapperClass(GrepMapper.class);

				//Output Key/Value
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);

				//Reducer Task
				job.setNumReduceTasks(0);

				//Set Parameters
				job.getConfiguration().set("target",args[++i]);
			} else if (args[i].equals("-group")){
				//Mapper Class
				job.setMapperClass(GroupMapper.class);

				//Reducer Class
				job.setReducerClass(GroupReducer.class);

				//Reducer Task
				job.setNumReduceTasks(1);

				//Output Key/Value
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				//Set Parameters
				job.getConfiguration().set("keyColumn",args[++i]);
				job.getConfiguration().set("valueColumn",args[++i]);
			}
		}
	}
}

