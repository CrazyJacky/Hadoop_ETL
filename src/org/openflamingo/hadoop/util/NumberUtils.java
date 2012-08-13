package org.openflamingo.hadoop.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * Number Utility.
 *
 * @author Edward KIM
 */
public class NumberUtils {

	private static DecimalFormat oneDecimal = new DecimalFormat("0");

	/**
	 * 소수점 둘째자리 포맷을 위한 Decimal Format
	 */
	private static final DecimalFormat decimalFormat;

	static {
		NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
		decimalFormat = (DecimalFormat) numberFormat;
		decimalFormat.applyPattern("#.##");
	}

	/**
	 * 지정한 정수를 사람이 읽기 편한 형태의 축약형 표현으로 변경한다.
	 * 지정한 크기를 계산하여 그 범위에 따라서 K, M, G, T로 변경한다.
	 *
	 * @param number 크기
	 * @return 사람이 읽기 편한 형태의 축약형 표현
	 */
	public static String humanReadableInt(long number) {
		long absNumber = Math.abs(number);
		double result = number;
		String suffix = "";
		if (absNumber < 1024) {
			// nothing
		} else if (absNumber < 1024 * 1024) {
			result = number / 1024.0;
			suffix = "K";
		} else if (absNumber < 1024 * 1024 * 1024) {
			result = number / (1024.0 * 1024);
			suffix = "M";
		} else if (absNumber < 1024 * 1024 * 1024 * 1024) {
			result = number / (1024.0 * 1024 * 1024);
			suffix = "G";
		} else {
			result = number / (1024.0 * 1024 * 1024 * 1024);
			suffix = "T";
		}
		return oneDecimal.format(result) + suffix;
	}

	/**
	 * Bytes의 길이를 축약형 표현으로 변경한다.
	 *
	 * @param len Bytes의 길이
	 * @return 축약형 표현
	 */
	public static String byteDesc(long len) {
		double val = 0.0;
		String ending = "";
		if (len < 1024 * 1024) {
			val = (1.0 * len) / 1024;
			ending = " KB";
		} else if (len < 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024 * 1024);
			ending = " MB";
		} else if (len < 1024L * 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024 * 1024 * 1024);
			ending = " GB";
		} else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
			ending = " TB";
		} else {
			val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
			ending = " PB";
		}
		return limitDecimalTo2(val) + ending;
	}

	/**
	 * 지정한 숫자를 소수점 둘째자리까지 포맷팅한다.
	 *
	 * @param d Double 크기의 숫자
	 * @return 소수점 둘째자리 포맷팅 문자열
	 */
	public static synchronized String limitDecimalTo2(double d) {
		return decimalFormat.format(d);
	}
}
