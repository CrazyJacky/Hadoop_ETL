package org.openflamingo.hadoop.generate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <code>CounterMapper</code> class is mapper for Generate ETL to count row for each Mapper.
 * A mapper can count number of read line by setting <code>Counter</code> using unique ID of current mapper
 * @author Hyunje
 */
public class CounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	/**
	 * Counter for a number of read line in current mapper.
	 */
	private Counter localCounter;
	/**
	 * Counter for a number of read line for whole mappers.
	 */
	private Counter globalCounter;

	/**
	 * Setups when executing Mapper. This function is called only once. Sets parameters.
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String[] orignID = context.getTaskAttemptID().toString().split("_");
		String mapperID = orignID[orignID.length - 2] + orignID[orignID.length - 1];
		localCounter = context.getCounter("LocalCounter".toUpperCase(), mapperID);
		globalCounter = context.getCounter("GlobalCounter".toUpperCase(), "Counter".toUpperCase());
	}

	/**
	 * <p>Runs <code>map()</code> for each line of input.</p>
	 * <p>Input key value is index of current line. And output key is null, value is text.</p>
	 * <p>Count number of rows</p>
	 * @param key input key of this mapper
	 * @param value input value of this mapper
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), NullWritable.get());
		localCounter.increment(1);
		globalCounter.increment(1);
	}
}
