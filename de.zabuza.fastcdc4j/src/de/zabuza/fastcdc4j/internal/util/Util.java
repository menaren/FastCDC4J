package de.zabuza.fastcdc4j.internal.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Daniel Tischner {@literal <zabuza.dev@gmail.com>}
 */
public final class Util {
	private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8);
	private static final MessageDigest SHA_1_HASHER = createSha1Hasher();

	public static String bytesToHex(byte[] bytes) {
		// See https://stackoverflow.com/a/9855338/2411243
		byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars, StandardCharsets.UTF_8);
	}

	private static MessageDigest createSha1Hasher() {
		try {
			return MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new AssertionError();
		}
	}

	public static byte[] hashSha1(byte[] data) {
		SHA_1_HASHER.reset();
		return SHA_1_HASHER.digest(data);
	}
}
