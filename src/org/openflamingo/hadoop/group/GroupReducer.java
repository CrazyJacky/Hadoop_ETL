package org.openflamingo.hadoop.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer for Group ETL
 *
 * @author Hyunje
 */
public class GroupReducer extends Reducer<Text, Text, NullWritable, Text> {
	private String outDelimiter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		outDelimiter = configuration.get("outdelimiter");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String outputString=key.toString() + " : ";
		Iterator<Text> iterator = values.iterator();
		while(iterator.hasNext()){
			outputString += iterator.next() + outDelimiter;
		}
		context.write(NullWritable.get(),new Text(outputString.substring(0,outputString.length()-1)));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
