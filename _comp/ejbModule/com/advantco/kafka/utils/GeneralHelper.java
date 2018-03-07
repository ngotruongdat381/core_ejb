package com.advantco.kafka.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;

import com.advantco.base.logging.LoggingLevel;

public final class GeneralHelper {
	public static final String VENDOR_NAME = "advantco.com";
	public static final String SOFTWARE_VERSION = "1.0.0";
	public static final String CORE_LOG_LOCATION = "/Applications/Advantco/KAFKAAdapter/Core";
	public static final String NEW_LINE = System.getProperty("line.separator");
	public static final String DELIMITER = ",";
	public static final int PREFIX_LENGTH = 12;
	public static final char[] CHARSET = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

	private static LoggingLevel logLevel = LoggingLevel.INFO;

	private GeneralHelper() {
	}

	public static LoggingLevel getLogLevel() {
		return logLevel;
	}

	public static void setLogLevel(LoggingLevel logLevel) {
		GeneralHelper.logLevel = logLevel;
	}

	public static boolean isUnix() {
		String os = System.getProperty("os.name").toLowerCase();
		return !(os.indexOf("win") >= 0);
	}

	public static Collection<String> parseStringToCollection(String content) {
		return Arrays.asList(content.split(DELIMITER));
	}

	public static String genRandomName() {
		String prefix = "";
		SecureRandom srandom = new SecureRandom();
		byte[] bytes = new byte[128];
		srandom.nextBytes(bytes);
		double range = srandom.nextDouble();
		for (int i = 0; i < PREFIX_LENGTH; i++) {
			int idx = (int) (range * CHARSET.length);
			prefix += CHARSET[idx];
		}
		return prefix;
	}

	public static String getGroupId() {
		return "Advantco Kafka Adapter Sender " + SOFTWARE_VERSION + " pid " + Thread.currentThread().getId() + "-" + System.currentTimeMillis();
	}

	public static String getClientId() {
		return "Advantco Kafka Adapter " + SOFTWARE_VERSION + " pid " + Thread.currentThread().getId() + "-" + System.currentTimeMillis();
	}

	public static String trace(Throwable throwable) {
		StringWriter sw = new StringWriter();
		sw.write("Throwable: ");
		throwable.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}

}
