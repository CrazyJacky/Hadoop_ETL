package org.openflamingo.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SampleReducer extends Reducer<Text, Text, NullWritable, Text> {

	private String outputDelimiter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		outputDelimiter = configuration.get("outputDelimiter");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text("Hello World"));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}