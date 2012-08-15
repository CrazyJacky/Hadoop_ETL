package hadoop.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Description.
 *
 * @author Hyunje
 * @version 1.0
 */
public class MainDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception{
		int res  = ToolRunner.run(new MainDriver(),args);
		System.exit(res);
	}

	@Override
	public int run(String[] strings) throws Exception {
		Job job = new Job();
		return 0;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
