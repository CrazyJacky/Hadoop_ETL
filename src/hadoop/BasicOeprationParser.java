package hadoop;

import java.util.HashMap;

/**
 * A mutable class of parser.
 *
 * @author Hyunje
 * @since 1.0
 * @version 1.0
 */
public abstract class BasicOeprationParser implements BasicParserInterface {
	int numOfCommonParameters = 5;
	String defaultINPUT;
	String defaultOUTPUT;
	String defaultJOBNAME;
	String defaultINDELIMITER;
	String defaultOUTDELIMITER;

	String operationName;
	int numOfAdditionalParameters;
	int sizeOfAdditionalParameters;

	HashMap<String, String> hashMap;

	public BasicOeprationParser(String name){
		operationName = name;
		numOfAdditionalParameters=0;
		sizeOfAdditionalParameters = 0;
		hashMap = new HashMap<String, String>();
	//	additionalParameters = new ArrayList<Parameter>();

		defaultINPUT = "hyunje/input/";
		defaultOUTPUT = "hyunje/output/"+operationName+"/";
		defaultJOBNAME = "Hadoop ETL";
		defaultINDELIMITER = ",";
		defaultOUTDELIMITER = ",";

		setDefaultParameters();

	}

	private void setDefaultParameters()
	{
		hashMap.put("input".toUpperCase(),defaultINPUT);
		hashMap.put("output".toUpperCase(),defaultOUTPUT);
		hashMap.put("jobname".toUpperCase(),defaultJOBNAME);
		hashMap.put("indelimiter".toUpperCase(),defaultINDELIMITER);
		hashMap.put("outdelimiter".toUpperCase(),defaultOUTDELIMITER);
	}

	public abstract void setNumOfAdditionalParameters(int num);
	public abstract int getNumOfAdditionalParameters();

	@Override
	public void addParameter(String paramName, String paramValue)
	{
		sizeOfAdditionalParameters++;
		hashMap.put(paramName,paramValue);
	}

	@Override
	public boolean checkParameters() {
		return sizeOfAdditionalParameters == numOfAdditionalParameters;
	}


}
