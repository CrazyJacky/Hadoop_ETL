package org.openflamingo.hadoop.replace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Replace
 *
 * @author Hyunje
 */
public class ReplaceMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String inDelimiter;
	private String outDelimiter;
	private String targetColumn;
	private String oldValue;
	private String newValue;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		inDelimiter = configuration.get("indelimiter");
		outDelimiter = configuration.get("outdelimiter");
		targetColumn = configuration.get("targetColumn");
		oldValue = configuration.get("oldValue");
		newValue = configuration.get("newValue");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(inDelimiter);
		String outputLine="";
		for(int i=0;i<line.length;i++)
		{
			if(i == Integer.parseInt(targetColumn))
			{
				if(line[i].equals(oldValue))
					line[i] = newValue;
			}
			outputLine = outputLine + line[i] + outDelimiter;
		}
		context.write(NullWritable.get(),new Text(outputLine.substring(0,outputLine.length()-1)));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
