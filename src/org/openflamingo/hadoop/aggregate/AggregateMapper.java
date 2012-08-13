package org.openflamingo.hadoop.aggregate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Aggregate
 *
 * @author Hyunje
 */
public class AggregateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), value);
	}
}