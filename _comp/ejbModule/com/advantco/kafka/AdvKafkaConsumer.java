package com.advantco.kafka;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;

import com.advantco.base.ThrowableUtil;
import com.advantco.base.logging.ILogger;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
//import com.advantco.kafka.adapter.error.ErrorStatus;
import com.advantco.kafka.builder.KConfigContext;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.record.KConsumerRecord;
import com.advantco.kafka.record.KRecordMetadata;
import com.advantco.kafka.utils.DumpLogs;
import com.advantco.kafka.utils.GeneralHelper;
import com.advantco.kafka.utils.KLoggerWrapper;
import com.sap.engine.interfaces.messaging.api.Message;

public class AdvKafkaConsumer<K, V> extends AdvKafkaConnection<K, V> {
	private static final ILogger logger = KLoggerWrapper.getLogger(AdvKafkaConsumer.class);
	private volatile KafkaConsumer<K, V> kafkaConsumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private AtomicBoolean donePoll = new AtomicBoolean(true);
	private Long pollIntervalTime;
	private Integer countKeepAlive;
	private KConfigContext kConfigRun;
	private static volatile KConnectionsHandlerImpl connectionAlives;

	public AdvKafkaConsumer(Class<K> recordKeyType, Class<V> recordValueType) {
		super(recordKeyType, recordValueType);
		countKeepAlive = null;
		kConfigRun = null;
		pollIntervalTime = 1L;
		connectionAlives = null;
	}

	@Override
	public void stop() throws KBaseException {
		try {
			cleanupResource();
			close();
		} catch (IOException ex) {
			throw new KBaseException(ex.getMessage(), ex);
		}
	}

	@Override
	public void close() throws IOException {
		if (kafkaConsumer != null) {
			closed.set(true);
			countKeepAlive = null;
			kafkaConsumer.wakeup();
			if (donePoll.get()) {
				kafkaConsumer.close();
				kafkaConsumer = null;
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("AdvKafkaConsumer: closed");
		}
	}

	@Override
	protected void cleanupResource() {
		getRunContext().cleanupResource();
	}

	@Override
	public boolean isOpened() {
		return kafkaConsumer != null;
	}

	@Override
	public boolean isRunning() {
		return kafkaConsumer != null && !donePoll.get();
	}

	@Override
	public void start() throws KBaseException {
	}

	@Override
	public void maintain() throws IOException, UnsupportedOperationException {
		throw new UnsupportedOperationException("maintain");
	}

	@Override
	public void open() throws IOException, GeneralSecurityException {
		Properties newProperties;
		try {
			Thread.currentThread().setContextClassLoader(KafkaException.class.getClassLoader());
			newProperties = getRunContext().buildConfigContext();
			if (logger.isDebugEnabled()) {
				logger.debug("AdvKafkaConsumer properties = " + DumpLogs.dumpHidePasswordProperties(newProperties).toString());
			}
			kafkaConsumer = getKafkaConsumer(newProperties);
			closed.set(false);
			if (countKeepAlive == null && !(Long.parseLong(kConfigRun.getConsumerConfigBuilder().getConnectionKeepAlive()) <= 0) && !(pollIntervalTime < 0)) {
				countKeepAlive = new Integer((int) (Long.parseLong(kConfigRun.getConsumerConfigBuilder().getConnectionKeepAlive()) / pollIntervalTime));
			}
		} catch (Exception ex) {
			throw new IOException(ex.getMessage(), ex);
		} finally {
			Thread.currentThread().setContextClassLoader(originalClassLoader);
		}
	}

	private void subscribe() {
		if (!closed.get()) {
			if (logger.isDebugEnabled()) {
				logger.debug("subscribing to topics : " + kConfigRun.getConsumerConfigBuilder().getTopics());
			}
			kafkaConsumer.subscribe(GeneralHelper.parseStringToCollection(kConfigRun.getConsumerConfigBuilder().getTopics()));
		}
	}

	@Override
	protected KRecordMetadata produce(String messageId, byte[] content, boolean isSynchronous) throws KBaseException {
		throw new UnsupportedOperationException("produce");
	}

	@Override
	protected Collection<KConsumerRecord<K, V>> consume() throws KBaseException {
		Collection<KConsumerRecord<K, V>> ret = new ArrayList<KConsumerRecord<K, V>>();
		Long pollTimeOut = Long.parseLong(getRunContext().getConsumerConfigBuilder().getPollTimeOut());
		try {
			donePoll.set(false);
			subscribe();

			if (connectionAlives == null) {
				connectionAlives = KConnectionsHandlerImpl.getNewInstance(pollIntervalTime - 2000L, this);
				connectionAlives.setLastFunctioningTime(System.currentTimeMillis());
				connectionAlives.start();
			} else {
				synchronized (connectionAlives) {
					connectionAlives.stop();
					connectionAlives.setLastFunctioningTime(System.currentTimeMillis());
					connectionAlives.start();
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("AdvKafkaConsumer: start poll : " + getRunContext().getConsumerConfigBuilder().getPollTimeOut());
			}
			ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeOut);
			if (logger.isDebugEnabled()) {
				logger.debug("the number of records  consumed : " + (records == null ? 0 : records.count()));
			}

			if (null != records && !records.isEmpty()) {
				Iterator<ConsumerRecord<K, V>> it = records.iterator();
				while (it.hasNext()) {
					ret.add(new KConsumerRecord<K, V>(it.next()));
				}
			}

			donePoll.set(true);
		} catch (WakeupException e) {
			if (!closed.get())
				throw e;
			if (logger.isDebugEnabled()) {
				logger.debug("AdvKafkaConsumer: Received Wakeup");
			}
			if (connectionAlives != null) {
				//if (ErrorStatus.getInstance().isHasErrorPoll()) {
					//KBaseException ex = new KBaseException(ErrorStatus.getInstance().getError() + "\n" + ThrowableUtil.trace(ErrorStatus.getInstance().getException(), GeneralHelper.SOFTWARE_VERSION));
					//ErrorStatus.getInstance().clear();
					//throw ex;
				//}
			}
		} catch (Exception ex) {
			throw new KBaseException(ex.getClass().getName() + ":" + ex.getMessage(), ex);
		} finally {
			if (kafkaConsumer != null) {
				kafkaConsumer.unsubscribe();

				if (closed.get() || countKeepAlive == null || countKeepAlive.intValue() == 0) {
					if (logger.isDebugEnabled()) {
						logger.debug("AdvKafkaConsumer: is closing");
					}
					kafkaConsumer.close();
					kafkaConsumer = null;
					countKeepAlive = null;
				} else {
					--countKeepAlive;
				}
			}
			if (connectionAlives != null) {
				synchronized (connectionAlives) {
					connectionAlives.stop();
				}
				connectionAlives = null;
			}
		}
		return ret;
	}

	private KafkaConsumer<K, V> getKafkaConsumer(Properties newProperties) {
		if (kafkaConsumer == null) {
			kafkaConsumer = new KafkaConsumer<K, V>(newProperties);
		}
		return kafkaConsumer;
	}

	public KConfigContext getRunContext() {
		if (kConfigRun == null) {
			kConfigRun = getContext();
		}
		return kConfigRun;
	}

	public void setPollIntervalTime(Long pollIntervalTime) {
		if (pollIntervalTime <= 0L) {
			pollIntervalTime = 1L;
		}
		this.pollIntervalTime = pollIntervalTime;
	}
	
	@Override
	public void resolveVarSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		kConfigRun = getContext().applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues);
	}

}
