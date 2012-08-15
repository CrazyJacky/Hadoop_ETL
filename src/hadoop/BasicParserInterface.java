package hadoop;

/**
 * A <code>BasicInterface</code> is a basical uniform for <code>BasicOperationParser</code>.
 *<p>
 * @author Hyunje
 * @version 1.0
 * @since 1.0
 */
public interface BasicParserInterface {
	/**
	 * Sets parameters. Number of parameters can be got by calling <code>getNumofParams()</code>
	 * in <code>BasicOperationParser</code>
	 */
	void setParameters();

	/**
	 *  Run this operation.
	 * @return the status of finished the operation.
	 */
	int Run();

	/**
	 * Check parameters of this operation are valid.
	 * @return validation of parameters
	 */
	boolean checkParameters();
}
