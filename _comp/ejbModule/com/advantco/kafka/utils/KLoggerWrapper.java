package com.advantco.kafka.utils;

import com.advantco.base.logging.ILogger;
import com.advantco.base.logging.LoggerFactory;

public class KLoggerWrapper {
	public static final String NW_CATEGORYLOGGER = "/Applications/Advantco/KAFKAAdapter";
	public static final String NW_LOGGERFACTORY = "NWLoggerFactory";

	private KLoggerWrapper() {
	}

	public static ILogger getLogger(String cateory, Class<?> clazz) {
		return LoggerFactory.getLogger(cateory, clazz.getName());
	}

	public static ILogger getLogger(String category, String location) {
		return LoggerFactory.getLogger(category, location);
	}

	public static ILogger getLogger(Class<?> clazz) {
		if (NW_LOGGERFACTORY.equals(LoggerFactory.getCurrentLoggerFactoryName())) {
			return getLogger(NW_CATEGORYLOGGER, clazz);
		}
		return getLogger(clazz.getName());
	}

	public static ILogger getLogger(String name) {
		if (NW_LOGGERFACTORY.equals(LoggerFactory.getCurrentLoggerFactoryName())) {
			return getLogger(NW_CATEGORYLOGGER, name);
		}
		return LoggerFactory.getLogger(name);
	}

}
