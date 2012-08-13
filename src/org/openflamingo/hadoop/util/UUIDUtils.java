package org.openflamingo.hadoop.util;

import java.util.UUID;

/**
 * Immutable Universally Unique Identifier (UUID)를 생성하는 Generator.
 * <p>
 * <pre>
 * String uuid = UUIDUtils.generateUUID();
 * </pre>
 * </p>
 *
 * @author Edward KIM
 */
public class UUIDUtils {

	/**
	 * 고유한 UUID를 생성한다.
	 *
	 * @return 생성된 UUID
	 */
	public static String generateUUID() {
		return UUID.randomUUID().toString();
	}

}