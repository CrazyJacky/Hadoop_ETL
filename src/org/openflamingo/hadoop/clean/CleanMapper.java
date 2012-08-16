package org.openflamingo.hadoop.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for Clean ETL.
 * <p>Clean ETL clears a column in a row.
 * <p>
 *     <pre>
 *         source : 1,aa,2,xxx
 *                  2,bb,1,yyy
 *                  3,cc,1,zzz
 *
 *         clear column 2 (start at 0)
 *
 *         after :  1,aa,xxx
 *                  2,bb,yyy
 *                  3,cc,zzz
 *     </pre>
 * </p>
 * @author hyunje
 */
public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	/**
	 * Delimiter for input
	 */
	private String inDelimiter;
	/**
	 * Delimiter for output
	 */
	private String outDelimiter;
	/**
	 * Target column to clear
	 */
	private String target;

	/**
	 * Setups when executing Mapper. This function is called only once. Sets parameters.
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		//need validation -> Driver로 위임.
		//input이 없을 때
		inDelimiter = configuration.get("indelimiter"); //get(String, default);
		outDelimiter = configuration.get("outdelimiter");
		target = configuration.get("target");
	}

	/**
	 * <p>Runs <code>map()</code> for each line of input.</p>
     * <p>Input key value is index of current line. And output key is null, value is text.</p>
	 * <p>Deletes target column</p>
	 * @param key input key of this mapper
	 * @param value input value of this mapper
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(inDelimiter);
		//a,b,c, -> 3개.
		String outputLine = "";
		for (int i = 0; i < line.length; i++) {
			if (Integer.parseInt(target) == i) continue;
			outputLine = outputLine + line[i] + outDelimiter;
		}
		context.write(NullWritable.get(), new Text(outputLine.substring(0, outputLine.length() - 1)));
	}
}
