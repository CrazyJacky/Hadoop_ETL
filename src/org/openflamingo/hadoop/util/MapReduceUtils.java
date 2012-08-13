package org.openflamingo.hadoop.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce Utility.
 *
 * @author Edward KIM
 */
public class MapReduceUtils {

	/**
	 * Comma Separated Input Path를 설정한다.
	 *
	 * @param job   Hadoop Job
	 * @param files 문자열 파일 경로
	 * @throws java.io.IOException 경로를 설정할 수 없는 경우
	 */
	public static void setFileInputPaths(Job job, String files) throws IOException {
		for (String file : files.split(",")) {
			FileInputFormat.addInputPath(job, new Path(file));
		}
	}

	/**
	 * String Array Input Path를 설정한다.
	 *
	 * @param job   Hadoop Job
	 * @param files 문자열 파일 경로
	 * @throws java.io.IOException 경로를 설정할 수 없는 경우
	 */
	public static void setFileInputPaths(Job job, String... files) throws IOException {
		for (String file : files) {
			FileInputFormat.addInputPath(job, new Path(file));
		}
	}

	/**
	 * String Input Path를 설정한다.
	 *
	 * @param job  Hadoop Job
	 * @param file 문자열 파일 경로
	 * @throws java.io.IOException 경로를 설정할 수 없는 경우
	 */
	public static void setFileInputPath(Job job, String file) throws IOException {
		FileInputFormat.addInputPath(job, new Path(file));
	}

	/**
	 * String Output Path를 설정한다.
	 *
	 * @param job  Hadoop Job
	 * @param file 문자열 파일 경로
	 * @throws java.io.IOException 경로를 설정할 수 없는 경우
	 */
	public static void setFileOutputPath(Job job, String file) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(file));
	}

	/**
	 * Mapper와 Reducer를 실행하는 JVM의 인자를 설정한다.
	 *
	 * @param job  Hadoop Job
	 * @param opts JVM Args
	 */
	public void setChildJavaOpts(Job job, String opts) {
		job.getConfiguration().set("mapred.child.java.opts", opts);
	}
}
