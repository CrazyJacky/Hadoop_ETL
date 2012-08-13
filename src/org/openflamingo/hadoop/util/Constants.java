package org.openflamingo.hadoop.util;

/**
 * Hadoop MapReduce Job에서 사용하는 각종 상수를 정의한 상수 클래스.
 *
 * @author Edward KIM
 */
public class Constants {

    /**
     * Default Delimiter
     */
    public static final String DEFAULT_DELIMITER = Delimiter.COMMA.getDelimiter();

    /**
     * Hadoop Job Fail
     */
    public static final int JOB_FAIL = -1;

    /**
     * Hadoop Job Success
     */
    public static final int JOB_SUCCESS = 0;

    /**
     * Total Row Count Counter Name
     */
    public static final String TOTAL_ROW_COUNT = "Total Row Count";

    /**
     * HDFS 임시 디렉토리 위치의 Key 값
     */
    public static final String TEMP_DIR = "tempDir";

    /**
     * YES
     */
    public static final String YES = "yes";

    /**
     * NO
     */
    public static final String NO = "no";
}
