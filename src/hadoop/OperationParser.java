package hadoop;

/**
 * Description.
 *
 * @author Hyunje
 * @version 1.0
 */
public class OperationParser extends BasicOeprationParser {

	String[] args;


	public OperationParser(String name)
	{
		super(name);
	}
	public void ParseArguments(String[] args)
	{
		this.args = args;
	}

	@Override
	public void setNumOfAdditionalParameters(int num) {
		this.numOfAdditionalParameters = num;
	}
	@Override
	public int getNumOfAdditionalParameters()
	{
		return numOfAdditionalParameters;
	}

	private void setInputPath(String[] paths)
	{
		hashMap.remove("input".toUpperCase());
		for(int i=0;i<paths.length;i++)
			hashMap.put("input".toUpperCase()+(i+1),paths[i]);
	}
	private void setOutputPath(String path)
	{
		hashMap.put("output".toUpperCase(),path);
	}
	private void setJobName(String jobName)
	{
		hashMap.put("jobname".toUpperCase(),jobName);
	}
	private void setIndelimiter(String delimiter)
	{
		hashMap.put("indelimiter".toUpperCase(),delimiter);
	}
	private void setOutdelimiter(String delimiter)
	{
		hashMap.put("outdelimiter".toUpperCase(),delimiter);
	}
}