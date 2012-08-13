package org.openflamingo.hadoop.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Group ETL
 *
 * @author Hyunje
 */
public class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String inDelimiter;

	private String keyColumn;
	private String valueColumn;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();

		inDelimiter = configuration.get("indelimiter");

		keyColumn = configuration.get("keyColumn");
		valueColumn = configuration.get("valueColumn");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(inDelimiter);
		String keyCol="";
		String valueCol="";
		for(int i=0;i<line.length;i++)
		{
			if(i == Integer.parseInt(keyColumn))
				keyCol = line[i];
			else if (i == Integer.parseInt(valueColumn))
				valueCol = line[i];
		}
		context.write(new Text(keyCol), new Text(valueCol));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
