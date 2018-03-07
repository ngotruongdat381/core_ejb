package com.advantco.kafka.utils;

import java.util.Properties;
import java.util.Map.Entry;

import org.apache.kafka.common.config.SaslConfigs;

public class DumpLogs {

	public static Properties dumpHidePasswordProperties(Properties properties) {
		Properties dumpProperties = new Properties();
		for (Entry<Object, Object> entry : properties.entrySet()) {
			String key = (String) entry.getKey();
			if (key.equals(SaslConfigs.SASL_JAAS_CONFIG)) {
				StringBuilder buildAuth = new StringBuilder();
				String[] authStr = ((String) entry.getValue()).split(" ");
				for (int i = 0; i < authStr.length; ++i) {
					if (authStr[i].toLowerCase().contains("password")) {
						buildAuth.append("password=******");
					} else {
						buildAuth.append(authStr[i]);
					}
					buildAuth.append(' ');
				}
				buildAuth.append(';');
				dumpProperties.put(key, buildAuth.toString());
			} else if (key.toLowerCase().contains("password")) {
				dumpProperties.put(key, "******");
			} else {
				dumpProperties.put(entry.getKey(), entry.getValue());
			}
		}
		return dumpProperties;
	}
}
