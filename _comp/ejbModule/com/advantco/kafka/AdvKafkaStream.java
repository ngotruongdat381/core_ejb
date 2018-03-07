package com.advantco.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
	
import com.advantco.base.logging.ILogger;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.ksql.KsqlQuery;
import com.advantco.kafka.ksql.KsqlResource;
import com.advantco.kafka.ksql.QueryStreaming;
import com.advantco.kafka.ksql.computation.Command;
import com.advantco.kafka.ksql.computation.CommandId;
import com.advantco.kafka.ksql.computation.CommandIdAssigner;
import com.advantco.kafka.ksql.computation.CommandRunner;
import com.advantco.kafka.ksql.computation.CommandStore;
import com.advantco.kafka.record.KConsumerRecord;
import com.advantco.kafka.record.KRecordMetadata;
import com.advantco.kafka.utils.KLoggerWrapper;
import com.fasterxml.jackson.core.Version;
import com.sap.engine.interfaces.messaging.api.Message;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
//import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;


public class AdvKafkaStream<K, V> extends AdvKafkaConnection<K, V> {
	private static final ILogger LOGGER = KLoggerWrapper.getLogger(AdvKafkaStream.class);

	private KsqlEngine ksqlEngine;
	private KsqlResource ksqlResource;
	private QueryStreaming queryStreaming;
	private CommandRunner commandRunner;
	private KConsumerRecord<String, byte[]> kafkaRecord;
	private Thread commandRunnerThread;

	private Thread takeGenericRowThread;
	private AtomicBoolean rowsWritten;
	private AtomicReference<Throwable> streamsException;

	private static AdminClient adminClient;
	private static final String COMMANDS_KSQL_TOPIC_NAME = "__KSQL_COMMANDS_TOPIC";
	private static final String COMMANDS_STREAM_NAME = "KSQL_COMMANDS";
	private KsqlQuery ksqlQuery;
	private String selectCommand = "";


	public AdvKafkaStream(Class<K> recordKeyType, Class<V> recordValueType) {
		super(recordKeyType, recordValueType);
		this.kafkaRecord = null;
		this.rowsWritten = new AtomicBoolean(false);
		this.streamsException = new AtomicReference<>(null);
	}

	public void setKsqlQuery(KsqlQuery ksqlQuery) {
		if ( null != ksqlQuery ) {
			this.ksqlQuery = ksqlQuery;
		}
	}

	public KsqlQuery getKsqlQuery() {
		return ksqlQuery;
	}

	@Override
	public void close() throws IOException {
		if ( null != adminClient ) {
			adminClient.close();
		}

		if ( null != ksqlEngine ) {
			ksqlEngine.terminateAllQueries();
			ksqlEngine.close();
		}

		if ( null != commandRunner ) {
			commandRunner.close();
			try {
				commandRunnerThread.join();
			} catch (InterruptedException ex) {
				if ( LOGGER.isDebugEnabled() ) {
					LOGGER.debug("Interrupted while waiting for CommandRunner thread to complete", ex);
				}
			}
		}

		if (takeGenericRowThread.isAlive()) {
			try {
				takeGenericRowThread.interrupt();
				takeGenericRowThread.join();
			} catch (InterruptedException exception) {
				if ( LOGGER.isDebugEnabled() ) {
					LOGGER.debug("Failed to join row writer thread");
				}
			}
		}
		if ( null != queryStreaming ) {
			queryStreaming.stopStream();
		}
	}

	@Override
	public boolean isOpened() {
		return ksqlEngine != null;
	}

	@Override
	public void maintain() throws IOException, UnsupportedOperationException {
		throw new UnsupportedOperationException("maintain");
	}

	@Override
	public void open() throws IOException, GeneralSecurityException {		
		try {
			registerClassLoader();
			
			Properties streamsProperties =  new Properties();
			getContext().getSecurityConfigBuilder().buildConfigProperties(streamsProperties);
			getContext().getStreamConfigBuilder().buildConfigProperties(streamsProperties);
			
			//			streamsProperties.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, ksqlQuery.getApplicationID());

			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("Stream properties: " + streamsProperties.toString());
			}
			
			KsqlConfig ksqlConfig = new KsqlConfig(streamsProperties);
			adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());			//There is no
			ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(adminClient));
			KafkaTopicClient client = ksqlEngine.getTopicClient();
			
			String commandTopic = getContext().getStreamConfigBuilder().getCommandTopic(); 
			
			client.createTopic(commandTopic, 1, (short) 1);
			LOGGER.debug(commandTopic);
			
			Map<String, Expression> commandTopicProperties = new HashMap<>();
			commandTopicProperties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("json"));
			commandTopicProperties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(commandTopic));
			ksqlEngine.getDDLCommandExec().execute(new RegisterTopicCommand(new RegisterTopic(
					QualifiedName.of(COMMANDS_KSQL_TOPIC_NAME),
					false,
					commandTopicProperties)));
			ksqlEngine.getDDLCommandExec().execute(new CreateStreamCommand("statementText", new CreateStream(
					QualifiedName.of(COMMANDS_STREAM_NAME),
					Collections.singletonList(new TableElement("STATEMENT", "STRING")),
					false,
					Collections.singletonMap(
							DdlConfig.TOPIC_NAME_PROPERTY,
							new StringLiteral(COMMANDS_KSQL_TOPIC_NAME)
							)), Collections.emptyMap(), ksqlEngine.getTopicClient()));
			
			Properties consProperties = new Properties();
			getContext().getConsumerConfigBuilder().buildConfigProperties(consProperties);
			KafkaConsumer<CommandId, Command> commandConsumer = new KafkaConsumer<>(
					consProperties,
					getJsonDeserializer(CommandId.class, true),
					getJsonDeserializer(Command.class, false)
					);
			// Producer Properties
			Properties prosProperties = new Properties();
			getContext().getProducerConfigBuilder().buildConfigProperties(prosProperties);

			KafkaProducer<CommandId, Command> commandProducer = new KafkaProducer<>(
					prosProperties,
					getJsonSerializer(true),
					getJsonSerializer(false)
					);
			CommandStore commandStore = new CommandStore(
					commandTopic,
					commandConsumer,
					commandProducer,
					new CommandIdAssigner(ksqlEngine.getMetaStore())
					);

			ksqlResource = new KsqlResource(ksqlEngine, commandStore);
			commandRunner = new CommandRunner(ksqlResource, commandStore);

			try {
				commandRunner.processPriorCommands();
			} catch (Exception e) {
				if ( LOGGER.isDebugEnabled() ) {
					LOGGER.debug("Prior Command exception " + e.getMessage());
				}
			}
			commandRunnerThread = new Thread(commandRunner);
			commandRunnerThread.start();	
			queryStreaming = new QueryStreaming(ksqlEngine, ksqlResource, streamsException);
		} catch (Exception ex) {
			if ( LOGGER.isWarnEnabled() ) {
				LOGGER.warn(ex.getMessage());
			}
			close();
		} finally {
			unRegisterClassLoader();
		}	
	}

	public void handleCreateCommand() throws Exception {
		try {
			registerClassLoader();
			String createCommand = ksqlQuery.buildCreateCommand();
			if ( createCommand.isEmpty() ) {
				LOGGER.info("Create Command is empty");
			} else {
				for ( SqlBaseParser.SingleStatementContext statementContext : new KsqlParser().getStatements(createCommand) ) {
					if ( statementContext.statement() instanceof SqlBaseParser.CreateStreamContext || 
							statementContext.statement() instanceof SqlBaseParser.CreateTableContext ) {
						String statementText = KsqlEngine.getStatementString(statementContext);
						try {
							queryStreaming.handleNonQuery(statementText);
						} catch ( Exception exc ) {
							throw new Exception(String.format( "Unable to execute statement '%s'", statementText));
						}
					}
				}
			}
		} catch (Exception ex) {
			if ( LOGGER.isWarnEnabled() ) {
				LOGGER.warn(ex.getMessage());
			}
			close();
		} finally {
			unRegisterClassLoader();
		}
	}

	public void handleSelectCommand() throws Exception {	
		try {		
			registerClassLoader();
			selectCommand = ksqlQuery.buildSelectCommand();
			if ( selectCommand.isEmpty() ) {
				throw new Exception("Select command is empty");
			} else {
				takeGenericRowThread = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							for ( SqlBaseParser.SingleStatementContext statementContext : new KsqlParser().getStatements(selectCommand) ) {
								String statementText = KsqlEngine.getStatementString(statementContext);
								if ( statementContext.statement() instanceof SqlBaseParser.QuerystatementContext ) {
									queryStreaming.handleQuery(statementText);
									try {
										while (true) {
//											String key = "tekey";
//											List<Object> listValue = new ArrayList<>();
//											listValue.add("Value");
//											GenericRow value = new GenericRow(listValue);
//											KeyValue<String, GenericRow>  resultQuery = new KeyValue<>(key, value);
											KeyValue<String, GenericRow> resultQuery = queryStreaming.getQueuedQueryMetadata().getRowQueue().take();
											LOGGER.info("Key: " + resultQuery.key + " Value: " + resultQuery.value);
											System.out.println("Key: " + resultQuery.key + " Value: " + resultQuery.value);
											ByteArrayOutputStream bos = new ByteArrayOutputStream();
											ObjectOutputStream oos = new ObjectOutputStream(bos);
											oos.writeObject(resultQuery.value.getColumns());
											oos.flush(); 
											oos.close(); 
											bos.close();
											kafkaRecord = new KConsumerRecord<String, byte[]>(new ConsumerRecord<String, byte[]>("", 1, 0, resultQuery.key , bos.toByteArray()));
											rowsWritten.set(true);
										}
									} catch ( InterruptedException exception ) {
										// Interrupt is used to end the thread
										System.out.println("Interrupt is used to end the thread " + exception.getCause());
										exception.printStackTrace();
										LOGGER.debug("Interrupt is used to end the thread");
									} catch ( Exception exception ) {
										// Would just throw the exception, but 1) can't throw checked exceptions from Runnable.run(),
										// and 2) seems easier than converting the exception into an unchecked exception and then
										// throwing it to a custom Thread.UncaughtExceptionHandler
										LOGGER.debug("Would just throw the exception");
										streamsException.compareAndSet(null, exception);
									}
								}
							}
						} catch (Exception ex) {
							throw new RuntimeException(ex); 						
						}
					}
				});
				takeGenericRowThread.start();
			}
		} finally {
			unRegisterClassLoader();
		}
	}

	public boolean hasRecord() throws Exception {
		try {
			while ( !rowsWritten.get() ) {
				Throwable exception = streamsException.get();
				if ( exception != null ) {
					throw exception;
				}
			}
			rowsWritten.set(false);
			return true;
		} catch ( Throwable ex ) {
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug(ex.getMessage());
			}
			return false;
		}
	}

//	public boolean hasRecordS() throws Exception {
//		boolean isRecorded = false;
//		String selectCommand = "";
//		try {
//			registerClassLoader();
//			selectCommand = ksqlQuery.buildSelectCommand();
//			if ( selectCommand.isEmpty() ) {
//				throw new Exception("Select command is empty");
//			} else {
//				for ( SqlBaseParser.SingleStatementContext statementContext : new KsqlParser().getStatements(selectCommand) ) {
//					String statementText = KsqlEngine.getStatementString(statementContext);
//					if ( statementContext.statement() instanceof SqlBaseParser.QuerystatementContext ) {
//						try {
//							KeyValue<String, GenericRow> resultQuery = queryStreaming.handleQuery(statementText);
//							LOGGER.info("Key: " + resultQuery.key + " Value: " + resultQuery.value);
//							System.out.println("Key: " + resultQuery.key + " Value: " + resultQuery.value);
//							ByteArrayOutputStream bos = new ByteArrayOutputStream();
//							ObjectOutputStream oos = new ObjectOutputStream(bos);
//							oos.writeObject(resultQuery.value.getColumns());
//							oos.flush(); 
//							oos.close(); 
//							bos.close();
//							kafkaRecord = new KConsumerRecord<String, byte[]>(new ConsumerRecord<String, byte[]>("", 1, 0, resultQuery.key , bos.toByteArray()));
//							isRecorded = true;
//						} catch ( InterruptedException ex ) {
//							if ( LOGGER.isWarnEnabled() ) {
//								LOGGER.warn("Terminate Thread");
//							}
//						} 
//					}
//				}
//			}
//		} catch (Exception ex) {
//			if ( LOGGER.isWarnEnabled() ) {
//				LOGGER.warn("Exception " + ex.getMessage() + " to execute statement " + selectCommand);
//			}
//			close();
//		} finally {
//			unRegisterClassLoader();
//		}
//		return isRecorded;
//	}

	public KConsumerRecord<String, byte[]> getRecord() throws Exception{
		if ( null == kafkaRecord ) {
			throw new Exception("KConsumerRecord object is null");
		}
		return kafkaRecord;
	}

	private static <T> Serializer<T> getJsonSerializer(boolean isKey) {
		Serializer<T> result = new KafkaJsonSerializer<>();
		result.configure(Collections.emptyMap(), isKey);
		return result;
	}

	private static <T> Deserializer<T> getJsonDeserializer(Class<T> classs, boolean isKey) {
		Deserializer<T> result = new KafkaJsonDeserializer<>();
		String typeConfigProperty = isKey ? KafkaJsonDeserializerConfig.JSON_KEY_TYPE : KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;
		Map<String, ?> props = Collections.singletonMap(typeConfigProperty, classs);
		result.configure(props, isKey);
		return result;
	}

	public void registerClassLoader() {
		Thread.currentThread().setContextClassLoader(Version.class.getClassLoader());
	}

	public void unRegisterClassLoader() {
		Thread.currentThread().setContextClassLoader(originalClassLoader);
	}

	@Override
	public void resolveVarSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected KRecordMetadata produce(String messageId, byte[] content, boolean isSynchronous) throws KBaseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Collection<KConsumerRecord<K, V>> consume() throws KBaseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stop() throws KBaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start() throws KBaseException {
		// TODO Auto-generated method stub
		
	}
}
