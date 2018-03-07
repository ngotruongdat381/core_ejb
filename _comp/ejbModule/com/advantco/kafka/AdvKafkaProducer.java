package com.advantco.kafka;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.advantco.base.NameValuePair;
import com.advantco.base.StringUtil;
import com.advantco.base.logging.ILogger;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.builder.KConfigContext;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.record.KConsumerRecord;
import com.advantco.kafka.record.KRecordMetadata;
import com.advantco.kafka.utils.DumpLogs;
import com.advantco.kafka.utils.KLoggerWrapper;
import com.sap.engine.interfaces.messaging.api.Message;

public class AdvKafkaProducer<K, V> extends AdvKafkaConnection<K, V> {
	private static final ILogger logger = KLoggerWrapper.getLogger(AdvKafkaConsumer.class);
	private volatile KafkaProducer<K, V> kafkaProducer = null;
	private ProducerRecord<K, V> producerRecord = null;
	private volatile KConnectionsHandlerImpl connectionAlivessHandler;
	private KConfigContext kConfigRun;

	public AdvKafkaProducer(Class<K> recordKeyType, Class<V> recordValueType) {
		super(recordKeyType, recordValueType);
		connectionAlivessHandler = null;
		kConfigRun = null;
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
		if (kafkaProducer != null) {
			kafkaProducer.flush();
			kafkaProducer.close();
			kafkaProducer = null;
		}
		if (connectionAlivessHandler != null) {
			synchronized (connectionAlivessHandler) {
				if (logger.isDebugEnabled()) {
					logger.debug("AdvKafkaProducer ConnectionAlivessHandler stop()");
				}
				connectionAlivessHandler.stop();
			}
			connectionAlivessHandler = null;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("AdvKafkaProducer closed");
		}
	}

	@Override
	protected void cleanupResource() {
		getRunContext().cleanupResource();
	}

	@Override
	public boolean isOpened() {
		return kafkaProducer != null;
	}

	@Override
	public boolean isRunning() {
		return kafkaProducer != null;
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
			newProperties = getRunContext().buildConfigContext();

			if (logger.isDebugEnabled()) {
				logger.debug("AdvKafkaProducer properties = " + DumpLogs.dumpHidePasswordProperties(newProperties).toString());
			}

			Thread.currentThread().setContextClassLoader(KafkaException.class.getClassLoader());
			kafkaProducer = getKafkaProducer(newProperties);

			if (!(Long.parseLong(kConfigRun.getProducerConfigBuilder().getConnectionKeepAlive()) <= 0)) {
				connectionAlivessHandler = KConnectionsHandlerImpl.getInstance(Long.parseLong(kConfigRun.getProducerConfigBuilder().getConnectionKeepAlive()), this);
				connectionAlivessHandler.setLastFunctioningTime(System.currentTimeMillis());
			}

		} catch (Exception ex) {
			throw new IOException(ex.getMessage(), ex);
		} finally {
			Thread.currentThread().setContextClassLoader(originalClassLoader);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected KRecordMetadata produce(String messageId, byte[] content, boolean isSynchronous) throws KBaseException {
		KRecordMetadata resultRecord = null;
		String topic = kConfigRun.getProducerConfigBuilder().getTopic();
		String partition = kConfigRun.getProducerConfigBuilder().getPartition();

		try {
			producerRecord = (ProducerRecord<K, V>) new ProducerRecord<String, byte[]>(topic, StringUtil.nullOrBlank(partition) ? null : Integer.valueOf(partition), null, messageId, content);
			if (kConfigRun.getProducerConfigBuilder().getRecordHeaders() != null) {
				Iterator<NameValuePair> it = kConfigRun.getProducerConfigBuilder().getRecordHeaders().iterator();
				while (it.hasNext()) {
					NameValuePair config = (NameValuePair) it.next();
					producerRecord.headers().add(new RecordHeader(config.getName(), config.getValue().getBytes()));
				}
			}
			// if(context.getProducerConfigBuilder().isEnableTransactionParameter()
			// &&
			// StringUtil.notNullNorBlank(context.getProducerConfigBuilder().getTransactionId()))
			// {
			// kafkaProducer.initTransactions();
			// kafkaProducer.beginTransaction();
			// }
			Future<RecordMetadata> futureRecord = kafkaProducer.send(producerRecord);

			if (isSynchronous) {
				resultRecord = new KRecordMetadata(futureRecord.get());
				if (logger.isDebugEnabled()) {
					logger.debug("returned record metadata : " + resultRecord.toString());
				}
			} else {
				futureRecord.get();
			}
		} catch (InterruptedException e) {
			throw new KBaseException(e.getMessage(), e);
		} catch (ExecutionException e) {
			throw new KBaseException(e.getMessage(), e);
		} catch (Exception ex) {
			throw new KBaseException(ex.getMessage(), ex);
		} finally {
			// if(context.getProducerConfigBuilder().isEnableTransactionParameter()
			// &&
			// StringUtil.notNullNorBlank(context.getProducerConfigBuilder().getTransactionId()))
			// {
			// kafkaProducer.commitTransaction();
			// }
			if (connectionAlivessHandler == null) {
				if (logger.isDebugEnabled()) {
					logger.debug("AdvKafkaProducer is closing");
				}
				kafkaProducer.close();
			} else {

				synchronized (connectionAlivessHandler) {
					if (!connectionAlivessHandler.isRunning()) {
						connectionAlivessHandler.start();
					}
				}
			}
		}
		return resultRecord;
	}

	private KafkaProducer<K, V> getKafkaProducer(Properties newProperties) {
		if (kafkaProducer == null) {
			kafkaProducer = new KafkaProducer<K, V>(newProperties);
		}
		return kafkaProducer;
	}

	public KConfigContext getRunContext() {
		if (kConfigRun == null) {
			kConfigRun = getContext();
		}
		return kConfigRun;
	}

	@Override
	protected Collection<KConsumerRecord<K, V>> consume() {
		throw new UnsupportedOperationException("consume");
	}

	@Override
	public void resolveVarSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		kConfigRun = getContext().applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues);
	}
}