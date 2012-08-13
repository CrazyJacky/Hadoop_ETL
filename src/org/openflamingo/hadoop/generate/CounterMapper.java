package org.openflamingo.hadoop.generate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Generate ETL to count row for each Mapper.
 *
 * @author Hyunje
*/
public class CounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	private Counter localCounter;
	private Counter globalCounter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String[] orignID = context.getTaskAttemptID().toString().split("_");
		String mapperID = orignID[orignID.length - 2] + orignID[orignID.length - 1];
		localCounter = context.getCounter("LocalCounter".toUpperCase(), mapperID);
		globalCounter = context.getCounter("GlobalCounter".toUpperCase(),"Counter".toUpperCase());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(),NullWritable.get());
		localCounter.increment(1);
		globalCounter.increment(1);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
