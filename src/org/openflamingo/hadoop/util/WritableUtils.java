package org.openflamingo.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Writable Object를 처리하는 유틸리티.
 *
 * @author Edward KIM
 */
public class WritableUtils {

	/**
	 * Byte Array에서 Writable Object로 값을 로딩한다.
	 *
	 * @param byteArray      Writable Object를 구성하는 Byte Array
	 * @param writableObject 값을 채워넣을 Writable Object
	 */
	public static void readFieldsFromByteArray(byte[] byteArray, Writable writableObject) {
		DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteArray));
		try {
			writableObject.readFields(inputStream);
		} catch (IOException e) {
			throw new IllegalStateException("Byte Array의 값을 Writable로 설정할 수 없습니다: IOException", e);
		}
	}

	/**
	 * Object를 Byte Array로 직렬화 한다.
	 *
	 * @param writableObject Byte Array로 직렬화할 Writable Object
	 * @return Byte Array
	 */
	public static byte[] writeToByteArray(Writable writableObject) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutput output = new DataOutputStream(outputStream);
		try {
			writableObject.write(output);
		} catch (IOException e) {
			throw new IllegalStateException("직렬화할 수 없습니다: IOStateException", e);
		}
		return outputStream.toByteArray();
	}

	/**
	 * Writable List를 Byte Array로 직렬화한다.
	 *
	 * @param writableList Byte Array로 직렬화할 Writable Object List
	 * @return Byte Array
	 */
	public static byte[] writeListToByteArray(List<? extends Writable> writableList) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutput output = new DataOutputStream(outputStream);
		try {
			output.writeInt(writableList.size());
			for (Writable writable : writableList) {
				writable.write(output);
			}
		} catch (IOException e) {
			throw new IllegalStateException("직렬화할 수 없습니다: IOException", e);
		}
		return outputStream.toByteArray();
	}

	/**
	 * Byte Array를 Writable Object의 List로 역직렬화한다
	 *
	 * @param byteArray     Writable Object List의 필드값을 구성하는 Byte Array
	 * @param writableClass Writable Class
	 * @param conf          Hadoop Configuration
	 * @return LWritable Object의 List
	 */
	public static List<? extends Writable> readListFieldsFromByteArray(byte[] byteArray, Class<? extends Writable> writableClass, Configuration conf) {
		try {
			DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteArray));
			int size = inputStream.readInt();
			List<Writable> writableList = new ArrayList<Writable>(size);
			for (int i = 0; i < size; ++i) {
				Writable writable = ReflectionUtils.newInstance(writableClass, conf);
				writable.readFields(inputStream);
				writableList.add(writable);
			}
			return writableList;
		} catch (IOException e) {
			throw new IllegalStateException("역직렬화할 수 없습니다: IOException", e);
		}
	}
}
