package com.advantco.kafka.ksql;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.advantco.base.logging.ILogger;
import com.advantco.kafka.ksql.computation.CommandStatus;
import com.advantco.kafka.ksql.entity.CommandStatusEntity;
import com.advantco.kafka.ksql.entity.KsqlEntity;
import com.advantco.kafka.ksql.entity.KsqlEntityList;
import com.advantco.kafka.utils.KLoggerWrapper;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;

public class QueryStreaming {
	private static final ILogger LOGGER = KLoggerWrapper.getLogger(QueryStreaming.class);
	private KsqlEngine ksqlEngine;
	private KsqlResource ksqlResource;
	private QueuedQueryMetadata queuedQueryMetadata;
	private KsqlEntityList ksqlEntities;
	private AtomicReference<Throwable> streamsException;
	
	public QueryStreaming(KsqlEngine ksqlEngine, KsqlResource ksqlResource, AtomicReference<Throwable> streamsException) {
		this.ksqlEngine = ksqlEngine;
		this.ksqlResource = ksqlResource;
		
		ksqlEntities = new KsqlEntityList();
		this.streamsException = streamsException;
	}

	public void handleNonQuery(String ksqlString) throws Exception{
		try {
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("Create command = " + ksqlString);
			}
			List<Statement> parsedStatements = ksqlEngine.getStatements(ksqlString);
			List<String> statementStrings = ksqlResource.getStatementStrings(ksqlString);
			if ( parsedStatements.size() != statementStrings.size() ) {
				throw new Exception(String.format(
						"Size of parsed statements and statement strings differ; %d vs. %d, respectively",
						parsedStatements.size(),
						statementStrings.size()
						));
			}

			for ( int i = 0; i < parsedStatements.size(); i++ ) {
				String statementText = statementStrings.get(i);
				ksqlEntities.add(ksqlResource.executeStatement(statementText, 
						parsedStatements.get(i), Collections.emptyMap()));
			}
		} catch ( Exception exception ) {
			LOGGER.info(exception.getMessage());
		}
	}

	public void handleQuery(String ksqlString) throws Exception {
		for ( KsqlEntity entity : ksqlEntities ) {
			if ( entity instanceof CommandStatusEntity &&
	              (((CommandStatusEntity) entity).getCommandStatus().getStatus() != CommandStatus.Status.SUCCESS)) {
	          
	          throw new Exception(String.format(
	        		  "Create Command has not executed yet, current status %s", 
	        		  ((CommandStatusEntity) entity).getCommandStatus().getMessage()));
	        }
		}
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("Query command = " + ksqlString);
		}
		QueryMetadata queryMetadata = ksqlEngine.buildMultipleQueries(ksqlString, Collections.emptyMap()).get(0);
		if ( !(queryMetadata instanceof QueuedQueryMetadata) ) {
			throw new Exception(String.format(
					"Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
					queryMetadata.getClass()));
		}
		queuedQueryMetadata = ((QueuedQueryMetadata) queryMetadata);
		queuedQueryMetadata.getKafkaStreams().setUncaughtExceptionHandler(new StreamsExceptionHandler());
		queryMetadata.getKafkaStreams().start();
		LOGGER.info("start streaming");
	}
	
	public QueuedQueryMetadata getQueuedQueryMetadata(){
		return queuedQueryMetadata;
	}
	
	private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
	    @Override
	    public void uncaughtException(Thread thread, Throwable exception) {
	      streamsException.compareAndSet(null, exception);
	    }
	  }
//	public void startStream() {
//		if ( null != queryMetadata ) {
//			queryMetadata.getKafkaStreams().start();
//		}
//	}
//
	public void stopStream() {
		if ( null != queuedQueryMetadata ) {
			queuedQueryMetadata.getKafkaStreams().close(100L, TimeUnit.MILLISECONDS);
			queuedQueryMetadata.getKafkaStreams().cleanUp();
		}
	}
}
