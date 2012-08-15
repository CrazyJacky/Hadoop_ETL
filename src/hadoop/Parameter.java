package hadoop;

/**
 * The <code>Parameter</code> class represents parameter of each <code>BaseOperation</code> classes.
 * Each operations has parameters or not. Classes which are needing parameters will have
 * this object as member.
 * <p>
 * This class can be used as follows(in <code>clean</code> operation):
 * <p><blockquote><pre>
 *     Parameter targetColumn = new Parameter("targetColumn",0);
 * </pre></blockquote></p>
 *
 * @author Hyunje
 * @version 1.0
 * @since 1.0
 */
public class Parameter {
	/** The value is used for name of parameter. */
	String paramName;
	/** This is the value of parameter. */
	String paramValue;

	/**
	 * Initialize as null values.
	 */
	public Parameter()
	{
		paramName = null;
		paramValue = null;
	}

	/**
	 * Initialize using argumnets.
	 * @param paramName
	 *          Name of this parameter
	 * @param paramValue
	 *          Value of this paramter
	 */
	public Parameter(String paramName, String paramValue)
	{
		this.paramName = paramName;
		this.paramValue = paramValue;
	}

	/**
	 * Set paramter name and value using arguments
	 * @param paramName
	 *          Name of parameter
	 * @param paramValue
	 *          Value of parameter
	 */
	public void setParameter(String paramName, String paramValue)
	{
		this.paramName = paramName;
		this.paramValue = paramValue;
	}
}
