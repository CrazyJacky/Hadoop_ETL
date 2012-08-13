package org.openflamingo.hadoop.grep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Grep
 *
 * TODO] Regular expression. -> pattern matching
 *
 * @author Hyunje
 */
public class GrepMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	String inputDelimiter;
	String outputDelimiter;
	String target;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		inputDelimiter = configuration.get("indelimiter");
		outputDelimiter = configuration.get("outdelimiter");
		target = configuration.get("target");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lines[] = value.toString().split(inputDelimiter);
		String outputLine = "";
		boolean checked = false;
		for (String line : lines) {
			if (line.equals(target))
				checked = true;
		}

		if (checked) {
			for (String aLine : lines) {
				outputLine = outputLine + aLine + outputDelimiter;
			}
			context.write(NullWritable.get(), new Text(outputLine.substring(0, outputLine.length() - 1)));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
