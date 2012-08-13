package org.openflamingo.hadoop.util;

import java.util.Iterator;

/**
 * {@link java.lang.Iterable}의 동일함을 비교하는 유틸리티.
 *
 * @author Edward KIM
 */
public class IterableComparisonUtils {

	/**
	 * Iterable이 동일한지 확인한다. Iterable의 값은 순서까지 동일해야 한다.
	 *
	 * @param <T>    비교할 Iterable의 유형
	 * @param first  비교할 첫번째 Iterable
	 * @param second 비교할 두번째 Iterable
	 * @return 동일하다면 <tt>true</tt>
	 */
	public static <T> boolean equal(Iterable<T> first, Iterable<T> second) {
		return equal(first.iterator(), second.iterator());
	}

	/**
	 * Iterable이 동일한지 확인한다. Iterable의 값은 순서까지 동일해야 한다.
	 *
	 * @param <T>    비교할 Iterable의 유형
	 * @param first  비교할 첫번째 Iterable
	 * @param second 비교할 두번째 Iterable
	 * @return 동일하다면 <tt>true</tt>
	 */
	public static <T> boolean equal(Iterator<T> first, Iterator<T> second) {
		while (first.hasNext() && second.hasNext()) {
			T message = first.next();
			T otherMessage = second.next();
			/* Element가 같아야 함 */
			if (!(message == null ? otherMessage == null :
				message.equals(otherMessage))) {
				return false;
			}
		}
		/* 길이가 같아야 함 */
		return !(first.hasNext() || second.hasNext());
	}
}
