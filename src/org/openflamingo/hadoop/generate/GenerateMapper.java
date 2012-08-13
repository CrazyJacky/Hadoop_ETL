package org.openflamingo.hadoop.generate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Description.
 *
 * @author Hyunje
 */
public class GenerateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String inDelimiter;
	private String outDelimiter;
	private int startIndex;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		String orignID[] = context.getTaskAttemptID().toString().split("_");
		String mapperID = orignID[orignID.length - 2] + orignID[orignID.length - 1];

		inDelimiter = configuration.get("indelimiter");
		outDelimiter = configuration.get("outdelimiter");

		String paramValue[] = configuration.get(mapperID).split(",");
		startIndex = Integer.parseInt(paramValue[0]);

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text(value + outDelimiter + startIndex));
		startIndex++;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
