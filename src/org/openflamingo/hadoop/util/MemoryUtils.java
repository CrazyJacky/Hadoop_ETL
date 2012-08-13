package org.openflamingo.hadoop.util;

/**
 * JVM Heap Memory Usage 유틸리티
 *
 * @author Edward KIM
 */
public class MemoryUtils {

	/**
	 * JVM의 Heap 정보를 문자열로 변환한다.
	 *
	 * @return JVM Heap 문자열
	 */
	public static String getRuntimeMemoryStats() {
		return "totalMem = " + (Runtime.getRuntime().totalMemory() / 1024f / 1024f) +
			"M, maxMem = " + (Runtime.getRuntime().maxMemory() / 1024f / 1024f) +
			"M, freeMem = " + (Runtime.getRuntime().freeMemory() / 1024f / 1024f) + "M";
	}

}
