package org.openflamingo.hadoop.aggregate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The class <code>AggregateMapper</code> is mapper for aggregate ETL.
 * <p></p>
 * Aggregate task can be done just reading each line for a map and writing that to context.
 * <p>
 *     <pre>
 *         source1 : 1,aa,xxx
 *                   2,bb,yyy
 *         source2 : 1,cc,zzz
 *                   2,dd,uuu
 *
 *         aggregate source1 and source2
 *
 *         result : 1,aa,xxx
 *                  2,bb,yyy
 *                  1,cc,zzz
 *                  2,dd,uuu
 *     </pre>
 * </p>
 * @author Hyunje
 */
public class AggregateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	/**
	 * Setups when executing Mapper.<p>
	 * This function is called only once.
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
	}

	/**
	 * <p>Runs <code>map()</code> for each line of input.</p>
	 * <p>Input key value is index of current line. And output key is null, value is text.</p>
	 * @param key input key of this mapper
	 * @param value input value of this mapper
	 * @param context context of current job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), value);
	}
}