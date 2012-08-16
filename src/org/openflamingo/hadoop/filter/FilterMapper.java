package org.openflamingo.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The <code>FilterMapper</code> class represents mapper for filter ETL. To filter a column, mapper needs
 * index of target column and query for that.
 * <p>Mapper writes read data when the query is true.
 * <p>
 *     <pre>
 *         source : 1,aa,xxx
 *                  2,bb,yyy
 *                  3,cc,xxx
 *                  4,dd,zzz
 *
 *         target : 2 (start at 0)
 *         query : EQ xxx
 *
 *         result : 1,aa,xxx
 *                  3,cc,xxx
 *     </pre>
 * </p>
 *
 * @author Hyunje
 */
public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	/**
	 * Delimiter for input
	 */
	String inDelimiter;
	/**
	 * Delimiter for output
	 */
	String outDelimiter;
	/**
	 * Target column to be filtered
	 */
	String targetColumn;

	/**
	 * Name of query
	 */
	String commandName;
	/**
	 * Value of query. In example, it is 'xxx'
	 */
	String commandValue;


	/**
	 * Setups when executing Mapper. This function is called only once. Sets parameters.
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();

		inDelimiter = configuration.get("indelimiter");
		outDelimiter = configuration.get("outdelimiter");
		targetColumn = configuration.get("targetColumn");

		//변수명의 규칙에 대한 고민
		commandName = configuration.get("commandName");
		commandValue = configuration.get("value");

		//hash map을 이용해서 한번에 하나씩 꺼내는 방식으로.
		//Filter의 핵심은 확장성.
	}

	/**
	 * <p>Runs <code>map()</code> for each line of input.</p>
     * <p>Input key value is index of current line. And output key is null, value is text.</p>
	 * <p>Filters target column</p>
	 * @param key input key of this mapper
	 * @param value input value of this mapper
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 *
	 * TODO{have to change using hashmap}
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(inDelimiter); //
		int target = Integer.parseInt(targetColumn); //
		String outputLine = "";
		if (commandName.equals("EMPTY".toLowerCase())) {
			if (line[target].isEmpty())
				outputLine = value.toString();
		} else if (commandName.equals("NEMPTY".toLowerCase())) {
			if (!line[target].isEmpty())
				outputLine = value.toString();
		} else if (commandName.equals("EQ".toLowerCase())) {
			if (line[target].equals(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("NEQ".toLowerCase())) {
			if (!line[target].equals(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("GT".toLowerCase())) {
			if (Double.parseDouble(line[target]) > Double.parseDouble(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("LT".toLowerCase())) {
			if (Double.parseDouble(line[target]) < Double.parseDouble(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("GTE".toLowerCase())) {
			if (Double.parseDouble(line[target]) >= Double.parseDouble(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("LTE".toLowerCase())) {
			if (Double.parseDouble(line[target]) <= Double.parseDouble(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("START".toLowerCase())) {
			if (line[target].endsWith(commandValue))
				outputLine = value.toString();
		} else if (commandName.equals("END".toLowerCase())) {
			if (line[target].endsWith(commandValue))
				outputLine = value.toString();
		}
		if (outputLine.length() > 0)
			context.write(NullWritable.get(), new Text(outputLine));
	}
}
