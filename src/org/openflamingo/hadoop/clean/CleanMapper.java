package org.openflamingo.hadoop.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Clean ETL
 *
 * @author hyunje
 */
public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String inDelimiter;
	private String outDelimiter;
	private String target;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	Configuration configuration = context.getConfiguration();
		inDelimiter = configuration.get("indelimiter");
		outDelimiter = configuration.get("outdelimiter");
		target = configuration.get("target");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(inDelimiter);
		String outputLine="";
		for(int i=0;i<line.length;i++)
		{
			if(Integer.parseInt(target) == i)   continue;
			outputLine = outputLine + line[i] + outDelimiter;
		}
		context.write(NullWritable.get(), new Text(outputLine.substring(0, outputLine.length() - 1)));
	}
}
