package com.advantco.kafka;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;

import javax.naming.NamingException;

import com.advantco.base.ConnectionAlivenessHandler;
import com.advantco.base.af.J2EEResourcesUtil;
import com.advantco.base.logging.ILogger;
import com.advantco.kafka.builder.KConfigContext;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.record.KConsumerRecord;
import com.advantco.kafka.record.KRecordMetadata;
import com.advantco.kafka.utils.KLoggerWrapper;
import com.advantco.kafka.varsubstitution.IVariableSubstitution;

public abstract class AdvKafkaConnection<K, V> implements ConnectionAlivenessHandler.IConnection, IVariableSubstitution {
	protected static ClassLoader originalClassLoader;
	private KConfigContext context;

	static {
		originalClassLoader = Thread.currentThread().getContextClassLoader();
	}

	public AdvKafkaConnection(Class<K> recordKeyType, Class<V> recordValueType) {
		getContext().getDataFormatBuilder().setRecordKeyType(recordKeyType);
		getContext().getDataFormatBuilder().setRecordValueType(recordValueType);
	}

	public final Collection<KConsumerRecord<K, V>> messageConsume() throws IOException, GeneralSecurityException, KBaseException {
		open();
		try {
			return consume();
		} finally {
		}
	}

	public final KRecordMetadata messageProduce(String messageId, byte[] content, String contentType, String charset, boolean isSynchronous) throws IOException, GeneralSecurityException, KBaseException {
		open();
		try {
			return produce(messageId, content, isSynchronous);
		} finally {
		}
	}

	protected abstract KRecordMetadata produce(String messageId, byte[] content, boolean isSynchronous) throws KBaseException;

	protected abstract Collection<KConsumerRecord<K, V>> consume() throws KBaseException;

	public abstract void stop() throws KBaseException;

	public abstract boolean isRunning();

	public abstract void start() throws KBaseException;

	public KConfigContext getContext() {
		if (context == null) {
			context = KConfigContext.KConfigContextFactory.getConfigContext();
		}
		return context;
	}

	public void setContext(KConfigContext context) {
		this.context = context;
	}

	protected void cleanupResource() {
		getContext().cleanupResource();
	}

	public static class KConnectionsHandlerImpl extends ConnectionAlivenessHandler {
		private static final ILogger logger = KLoggerWrapper.getLogger(KConnectionsHandlerImpl.class);
		private static KConnectionsHandlerImpl instance = null;
		private boolean running;

		public boolean isRunning() {
			return stopRequested != true && running == true;
		}

		private KConnectionsHandlerImpl(long maxIdleInterval, IConnection connection) {
			super(maxIdleInterval, ConnectionAlivenessHandler.Action.CLOSE, connection);
			lastFunctioningTime = System.currentTimeMillis();
		}

		public static KConnectionsHandlerImpl getInstance(long maxIdleInterval, IConnection connection) {
			if (instance == null) {
				instance = new KConnectionsHandlerImpl(maxIdleInterval, connection);
			}
			return instance;
		}

		public static KConnectionsHandlerImpl getNewInstance(long maxIdleInterval, IConnection connection) {
			return new KConnectionsHandlerImpl(maxIdleInterval, connection);
		}

		@Override
		public void stop() {
			running = false;
			super.stop();
		}

		@Override
		public void run() {
			running = true;
			super.run();
			running = false;
		}

		@Override
		public void start() {
			try {
				stopRequested = false;
				J2EEResourcesUtil.startRunnable(this);
			} catch (NamingException e) {
				handleException(e);
			}
		}

		@Override
		protected void handleException(Exception e) {
			logger.error("Starting connection alive handler failed", e);
		}
	}
}
