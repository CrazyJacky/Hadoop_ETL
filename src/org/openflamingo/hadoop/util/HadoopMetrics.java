package org.openflamingo.hadoop.util;


import java.util.Hashtable;
import java.util.Map;

/**
 * Hadoop MapReduce Metrics Resource Bundle.
 *
 * @author Edward KIM
 */
public class HadoopMetrics {

	public static Map<String, String> metrics = new Hashtable<String, String>();

	static {
		metrics.put("FileSystemCounters_FILE_BYTES_WRITTEN", "파일에 기록한 바이트");
		metrics.put("FileSystemCounters_HDFS_BYTES_WRITTEN", "HDFS에 기록한 바이트");
		metrics.put("FileSystemCounters_HDFS_BYTES_READ", "HDFS에서 읽은 바이트");
		metrics.put("FileSystemCounters_FILE_BYTES_READ", "파일에서 읽은 바이트");

		metrics.put("org.apache.hadoop.mapred.Task$Counter_MAP_INPUT_BYTES", "Mapper의 입력 바이트수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_REDUCE_INPUT_GROUPS", "Reducer의 입력 그룹수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_MAP_OUTPUT_RECORDS", "Mapper의 출력 레코드수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_COMBINE_INPUT_RECORDS", "Combiner의 입력 레코드수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_MAP_INPUT_RECORDS", "Mapper의 입력 레코드수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_MAP_OUTPUT_BYTES", "Mapper의 출력 바이트수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_REDUCE_OUTPUT_RECORDS", "Reducer의 출력 레코드수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_COMBINE_OUTPUT_RECORDS", "Combiner의 출력 레코드수");
		metrics.put("org.apache.hadoop.mapred.Task$Counter_REDUCE_INPUT_RECORDS", "Reducer의 입력 레코드수");
		metrics.put("org.apache.hadoop.mapred.lib.MultipleOutputs_SUPPORT", "Support");
		metrics.put("org.apache.hadoop.mapred.lib.MultipleOutputs_PREFIX", "Prefix");
		metrics.put("org.apache.hadoop.mapred.lib.MultipleOutputs_MEASURE", "Measure");
		metrics.put("org.apache.hadoop.mapred.JobInProgress$Counter_TOTAL_LAUNCHED_REDUCES", "총 실행된 Reducer의 개수");
		metrics.put("org.apache.hadoop.mapred.JobInProgress$Counter_TOTAL_LAUNCHED_MAPS", "총 실행된 Mapper의 개수");
	}

	public static String getMetricName(String key) {
		return metrics.get(key);
	}

}
