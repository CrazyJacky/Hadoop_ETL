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

import java.io.IOException;

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
			if (args[i].equals("-input"))
				FileInputFormat.addInputPaths(job, args[++i]);
			else if (args[i].equals("-output"))
				FileOutputFormat.setOutputPath(job, new Path(args[++i]));
			else if (args[i].equals("-jobName"))
				job.getConfiguration().set("mapred.job.name", args[++i]);
			else if (args[i].equals("-indelimiter"))
				job.getConfiguration().set("indelimiter", args[++i]);
			else if (args[i].equals("-outdelimiter"))
				job.getConfiguration().set("outdelimiter", args[++i]);
			else if (args[i].equals("-clean"))
				setCleanJob(args[++i]);
			else if (args[i].equals("-aggregate"))
				setAggregateJob(args[++i]);
			else if (args[i].equals("-replace"))
				setReplaceJob(args[++i],args[++i],args[++i]);
			else if (args[i].equals("-filter")){
				if(args.length-1-i == 3){
					setFilterJob(args[++i],args[++i],args[++i]);
				} else if (args.length-1-i == 2){
					setFilterJob(args[++i],args[++i]);
				}
			}
			else if (args[i].equals("-grep"))
				setGerpJob(args[++i]);
			else if (args[i].equals("-group"))
				setGroupjob(args[++i],args[++i]);
		}
	}

	private void setCleanJob(String target)
		{
			//Mapper Class
			job.setMapperClass(CleanMapper.class);

			//Output Key/Value
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			//Reducer Task
			job.setNumReduceTasks(0);

			//Set Parameter
			job.getConfiguration().set("target",target);

		}

		private void setAggregateJob(String aggPath) throws IOException {
			//Mapper Class
			job.setMapperClass(AggregateMapper.class);

			//Output Key/Value
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			//Reducer Task
			job.setNumReduceTasks(0);

			//Set Parameter
			FileInputFormat.addInputPaths(job, aggPath);
		}

		private void setReplaceJob(String targetColumn, String oldValue, String newVlaue)
		{
			//Mapper Class
			job.setMapperClass(ReplaceMapper.class);

			//Output Key/Value
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			//Reducer Task
			job.setNumReduceTasks(0);

			//Set Parameters
			job.getConfiguration().set("targetColumn",targetColumn);
			job.getConfiguration().set("oldValue",oldValue);
			job.getConfiguration().set("newValue",newVlaue);
		}

		private void setFilterJob(String targetColumn, String ... args)
		{
			// Mapper Class
			job.setMapperClass(FilterMapper.class);

			// Output Key/Value
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			// Reducer Task
			job.setNumReduceTasks(0);

			//Set Parameters
			//value : EMPTY, NEMPTY, EQ, NEQ, GT, LT, GTE, LTE, START, END
			job.getConfiguration().set("targetColumn",targetColumn);

			if(args.length == 1){
				if(args[0].equals("EMPTY") || args[0].equals("NEMPTY"))
				{
					job.getConfiguration().set("commandName", args[0].toLowerCase());
					job.getConfiguration().set("value","null");
				}
			} else if (args.length == 2){
				if(!(args[0].equals("EMPTY") || args[0].equals("NEMPTY")))
				{
					job.getConfiguration().set("commandName",args[0].toLowerCase());
					job.getConfiguration().set("value",args[1]);
				}
			}
		}

		private void setGerpJob(String target)
		{
			//Mapper Class
			job.setMapperClass(GrepMapper.class);

			//Output Key/Value
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			//Reducer Task
			job.setNumReduceTasks(0);

			//Set Parameters
			job.getConfiguration().set("target",target);
		}

		private void setGroupjob(String keyColumn, String valueColumn)
		{
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
			job.getConfiguration().set("keyColumn",keyColumn);
			job.getConfiguration().set("valueColumn",valueColumn);
		}
}

