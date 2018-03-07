package com.advantco.kafka.ksql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.common.errors.WakeupException;

import com.advantco.base.logging.ILogger;
import com.advantco.kafka.ksql.computation.Command;
import com.advantco.kafka.ksql.computation.CommandId;
import com.advantco.kafka.ksql.computation.CommandStatus;
import com.advantco.kafka.ksql.computation.CommandStatusFuture;
import com.advantco.kafka.ksql.computation.CommandStore;
import com.advantco.kafka.ksql.computation.RestoreCommands;
import com.advantco.kafka.ksql.entity.CommandStatusEntity;
import com.advantco.kafka.ksql.entity.KsqlEntity;
import com.advantco.kafka.ksql.entity.SourceDescription;
import com.advantco.kafka.ksql.entity.StreamList;
import com.advantco.kafka.ksql.entity.TableList;
import com.advantco.kafka.utils.KLoggerWrapper;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DDLCommand;
import io.confluent.ksql.ddl.commands.DDLCommandExec;
import io.confluent.ksql.ddl.commands.DDLCommandResult;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.ddl.commands.DropTopicCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.exception.ExceptionUtil;
//import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DDLStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import io.confluent.ksql.metastore.KsqlTopic;

import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ListProperties;

import io.confluent.ksql.util.KafkaTopicClient;
import org.slf4j.LoggerFactory;




public class KsqlResource {
	private static final ILogger LOGGER = KLoggerWrapper.getLogger(KsqlResource.class);
	public static final Long DISTRIBUTED_COMMAND_TIMEOUT_MS = 1000L;
	private final KsqlEngine ksqlEngine;
	private final CommandStore commandStore;
	
	public KsqlResource(KsqlEngine ksqlEngine, CommandStore commandStore) {
		this.ksqlEngine = ksqlEngine;
		this.commandStore = commandStore;
		registerDdlCommandTasks();
	}

	public KsqlEngine getKsqlEngine() {
		return ksqlEngine;
	}

	public CommandStore getCommandStore() {
		return commandStore;
	}

	public List<String> getStatementStrings(String ksqlString) {
		List<SqlBaseParser.SingleStatementContext> statementContexts = new KsqlParser().getStatements(ksqlString);
		List<String> result = new ArrayList<>(statementContexts.size());
		for (SqlBaseParser.SingleStatementContext statementContext : statementContexts) {
			// Taken from http://stackoverflow.com/questions/16343288/how-do-i-get-the-original-text-that-an-antlr4-rule-matched
			CharStream charStream = statementContext.start.getInputStream();
			result.add(
					charStream.getText(
							new Interval(
									statementContext.start.getStartIndex(),
									statementContext.stop.getStopIndex()
									)
							)
					);
		}
		return result;
	}

	public KsqlEntity executeStatement(String statementText, Statement statement, Map<String, Object> streamsProperties) throws Exception {
//		if ( statement instanceof ListStreams ) {
//			return listStreams(statementText);
//		} else if ( statement instanceof ListStreams ) {
//			return listTables(statementText);
//		} else if ( statement instanceof RegisterTopic
//				|| statement instanceof CreateStream
//				|| statement instanceof CreateTable
//				|| statement instanceof CreateStreamAsSelect
//				|| statement instanceof CreateTableAsSelect
//				|| statement instanceof TerminateQuery
//				|| statement instanceof DropTopic
//				|| statement instanceof DropStream
//				|| statement instanceof DropTable ) {
//			getStatementExecutionPlan(statement, statementText, streamsProperties);
//			return distributeStatement(statementText, statement, streamsProperties);
//		} else {
//			throw new Exception("Unable to execute statement");
//		}
		
			if (statement instanceof ListStreams) {
		      return listStreams(statementText);
		    } else if (statement instanceof ListTables) {
		      return listTables(statementText);
		    } else if (statement instanceof RunScript) {
		      return distributeStatement(statementText, statement, streamsProperties);
		    }else if (statement instanceof RegisterTopic
		            || statement instanceof CreateStream
		            || statement instanceof CreateTable
		            || statement instanceof CreateStreamAsSelect
		            || statement instanceof CreateTableAsSelect
		            || statement instanceof TerminateQuery
		            || statement instanceof DropTopic
		            || statement instanceof DropStream
		            || statement instanceof DropTable
		    ) {
		      if (statement instanceof AbstractStreamCreateStatement) {
		        AbstractStreamCreateStatement streamCreateStatement = (AbstractStreamCreateStatement)
		            statement;
		        Pair<AbstractStreamCreateStatement, String> avroCheckResult =
		            maybeAddFieldsFromSchemaRegistry(streamCreateStatement, streamsProperties);

		        if (avroCheckResult.getRight() != null) {
		          statement = avroCheckResult.getLeft();
		          statementText = avroCheckResult.getRight();
		        }
		      }
		      //Sanity check for the statement before distributing it.
		      validateStatement(statement, statementText, streamsProperties);
		      return distributeStatement(statementText, statement, streamsProperties);
		    } else {
		      if (statement != null) {
		        throw new KsqlException(String.format(
		            "Cannot handle statement of type '%s'",
		            statement.getClass().getSimpleName()
		        ));
		      } else {
		        throw new KsqlException(String.format(
		            "Unable to execute statement '%s'",
		            statementText
		        ));
		      }
		    }
	}
	
	  /**
	   * Validate the statement by creating the execution plan for it.
	   * 
	   * @param statement
	   * @param statementText
	   * @param streamsProperties
	   * @throws Exception
	   */
	  private void validateStatement(Statement statement, String statementText,
	                                 Map<String, Object> streamsProperties) throws KsqlException {
	    getStatementExecutionPlan(null, statement, statementText, streamsProperties);
	  }

	private void getStatementExecutionPlan(String queryId, Statement statement, String statementText, Map<String, Object> properties) throws KsqlException {
//		if ( statement instanceof Query ) {
//			ksqlEngine.getQueryExecutionPlan((Query) statement).getExecutionPlan();
//		} else if ( statement instanceof CreateStreamAsSelect ) {
//			CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
//			QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan(createStreamAsSelect.getQuery());
//			if ( queryMetadata.getDataSourceType() == DataSource.DataSourceType.KTABLE ) {
//				throw new KsqlException("Invalid result type. Your select query produces a TABLE. Please "
//						+ "use CREATE TABLE AS SELECT statement instead.");
//			}
//		} else if ( statement instanceof CreateTableAsSelect ) {
//			CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
//			QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan(createTableAsSelect.getQuery());
//			if ( queryMetadata.getDataSourceType() != DataSource.DataSourceType.KTABLE ) {
//				throw new KsqlException("Invalid result type. Your select query produces a STREAM. Please "
//						+ "use CREATE STREAM AS SELECT statement instead.");
//			}
//		} else if ( statement instanceof RegisterTopic ) {
//			RegisterTopic registerTopic = (RegisterTopic) statement;
//			RegisterTopicCommand registerTopicCommand = new RegisterTopicCommand(registerTopic, properties);
//			new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(registerTopicCommand);
//		} else if ( statement instanceof CreateStream ) {
//			CreateStream createStream = (CreateStream) statement;
//			CreateStreamCommand createStreamCommand = new CreateStreamCommand(createStream, properties, ksqlEngine.getTopicClient());
//			new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(createStreamCommand);
//		} else if ( statement instanceof CreateTable ) {
//			CreateTable createTable = (CreateTable) statement;
//			CreateTableCommand createTableCommand = new CreateTableCommand(createTable, properties, ksqlEngine.getTopicClient());
//			new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(createTableCommand);
//		} else if ( statement instanceof DropTopic ) {
//			DropTopic dropTopic = (DropTopic) statement;
//			DropTopicCommand dropTopicCommand = new DropTopicCommand(dropTopic);
//			new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropTopicCommand);
//		} else if ( statement instanceof DropStream ) {
//			DropStream dropStream = (DropStream) statement;
//			DropSourceCommand dropSourceCommand = new DropSourceCommand(dropStream);
//			new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropSourceCommand);
//		} else if ( statement instanceof DropTable ) {
//			DropTable dropTable = (DropTable) statement;
//			DropSourceCommand dropSourceCommand = new DropSourceCommand(dropTable);
//			new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropSourceCommand);
//		} else if ( statement instanceof TerminateQuery ) {
//			//			executionPlan = statement.toString();
//		} else {
//			throw new KsqlException("Cannot build execution plan for this statement.");
//		}
		
	    if (queryId != null) {
	        PersistentQueryMetadata metadata = ksqlEngine.getPersistentQueries().get(new QueryId(queryId));
	        if (metadata == null) {
	          throw new KsqlException(("Query with id:" + queryId + " does not exist, use SHOW QUERIES to view the full set of queries."));
	        }
	        KsqlStructuredDataOutputNode outputNode = (KsqlStructuredDataOutputNode) metadata.getOutputNode();
//	        return new SourceDescription(outputNode, metadata.getStatementString(), metadata.getStatementString(),
//	          metadata.getTopoplogy(), metadata.getExecutionPlan(), ksqlEngine.getTopicClient());
	      }


	      DDLCommandTask ddlCommandTask = ddlCommandTasks.get(statement.getClass());
	      if (ddlCommandTask != null) {
	        try {
	          String executionPlan = ddlCommandTask.execute(statement, statementText, properties);
//	          return new SourceDescription("", "User-Evaluation", Collections.EMPTY_LIST,
//	            Collections.EMPTY_LIST, Collections.EMPTY_LIST, "QUERY", "", "", "",
//	              "", true, "", "", "", executionPlan, 0, 0);
	        } catch (KsqlException ksqlException) {
	          throw ksqlException;
	        } catch (Throwable t) {
	          throw new KsqlException("Cannot RUN execution plan for this statement, " + statement, t);
	        }
	      }
	      throw new KsqlException("Cannot FIND execution plan for this statement:" + statement);
	}
	
	private interface DDLCommandTask {
		String execute(Statement statement, String statementText, Map<String, Object> properties) throws Exception;
	}
	
	private Map<Class, DDLCommandTask> ddlCommandTasks = new HashMap<>();

	private void registerDdlCommandTasks() {
	    ddlCommandTasks.put(Query.class, (statement, statementText, properties) -> ksqlEngine.getQueryExecutionPlan((Query) statement).getExecutionPlan());

	    ddlCommandTasks.put(CreateStreamAsSelect.class, (statement, statementText, properties) -> {
	      QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan(((CreateStreamAsSelect) statement).getQuery());
	      if (queryMetadata.getDataSourceType() == DataSource.DataSourceType.KTABLE) {
	        throw new KsqlException("Invalid result type. Your SELECT query produces a TABLE. Please "
	                                + "use CREATE TABLE AS SELECT statement instead.");
	      }
	      if (queryMetadata instanceof PersistentQueryMetadata) {
	        new AvroUtil().validatePersistentQueryResults((PersistentQueryMetadata) queryMetadata,
	                                                      ksqlEngine.getSchemaRegistryClient());
	      }
	      queryMetadata.close();
	      return queryMetadata.getExecutionPlan();
	    });

	    ddlCommandTasks.put(CreateTableAsSelect.class, (statement, statementText, properties) -> {
	      QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan(((CreateTableAsSelect) statement).getQuery());
	      if (queryMetadata.getDataSourceType() != DataSource.DataSourceType.KTABLE) {
	        throw new KsqlException("Invalid result type. Your SELECT query produces a STREAM. Please "
	                + "use CREATE STREAM AS SELECT statement instead.");
	      }
	      if (queryMetadata instanceof PersistentQueryMetadata) {
	        new AvroUtil().validatePersistentQueryResults((PersistentQueryMetadata) queryMetadata,
	                                                      ksqlEngine.getSchemaRegistryClient());
	      }
	      queryMetadata.close();
	      return queryMetadata.getExecutionPlan();
	    });

	    ddlCommandTasks.put(RegisterTopic.class, (statement, statementText, properties) -> {
	      RegisterTopicCommand registerTopicCommand = new RegisterTopicCommand((RegisterTopic) statement, properties);
	      new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(registerTopicCommand);
	      return statement.toString();
	    });

	    ddlCommandTasks.put(CreateStream.class, (statement, statementText, properties) -> {
	      CreateStreamCommand createStreamCommand =
	              new CreateStreamCommand(statementText, (CreateStream) statement, properties, ksqlEngine.getTopicClient());
	      executeDDLCommand(createStreamCommand);
	      return statement.toString();
	    });

	    ddlCommandTasks.put(CreateTable.class, (statement, statementText, properties) -> {
	      CreateTableCommand createTableCommand =
	          new CreateTableCommand(statementText, (CreateTable) statement, properties, ksqlEngine.getTopicClient());
	      executeDDLCommand(createTableCommand);
	      return statement.toString();
	    });

	    ddlCommandTasks.put(DropTopic.class, (statement, statementText, properties) -> {
	      DropTopicCommand dropTopicCommand = new DropTopicCommand((DropTopic) statement);
	      new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropTopicCommand);
	      return statement.toString();

	    });

	    ddlCommandTasks.put(DropStream.class, (statement, statementText, properties) -> {
	      DropSourceCommand dropSourceCommand = new DropSourceCommand((DropStream) statement, DataSource.DataSourceType.KSTREAM, ksqlEngine);
	      executeDDLCommand(dropSourceCommand);
	      return statement.toString();
	    });

	    ddlCommandTasks.put(DropTable.class, (statement, statementText, properties) -> {
	      DropSourceCommand dropSourceCommand = new DropSourceCommand((DropTable) statement, DataSource.DataSourceType.KTABLE, ksqlEngine);
	      executeDDLCommand(dropSourceCommand);
	      return statement.toString();
	    });

	    ddlCommandTasks.put(TerminateQuery.class, (statement, statementText, properties) -> statement.toString());
	  }
	
	private CommandStatusEntity distributeStatement(String statementText, Statement statement, Map<String, Object> streamsProperties) throws Exception {
		CommandId commandId = commandStore.distributeStatement(statementText, statement, streamsProperties);
		CommandStatus commandStatus;
		try {
			commandStatus = commandStore.registerQueuedCommand(commandId).get(DISTRIBUTED_COMMAND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		} catch (TimeoutException exception) {
			if ( LOGGER.isWarnEnabled() ) {
				LOGGER.warn("Timeout to get commandStatus, waited " + DISTRIBUTED_COMMAND_TIMEOUT_MS);	
			}  
			commandStatus = commandStore.getStatus(commandId);
		}
		return new CommandStatusEntity(statementText, commandId, commandStatus);
	}

//	public void handleStatement(Command command, CommandId commandId) throws Exception {
//		handleStatementWithTerminatedQueries(command, commandId, null);
//	}

//	public void handleStatements(List<Pair<CommandId, Command>> priorCommands) throws Exception {
//		for (Pair<CommandId, Command> commandIdCommandPair: priorCommands) {
//			try {
//				handleStatementWithTerminatedQueries(
//						commandIdCommandPair.getRight(),
//						commandIdCommandPair.getLeft(),
//						Collections.emptyMap()
//						);
//			} catch (Exception exception) {
//				if ( LOGGER.isWarnEnabled() ) {
//					LOGGER.warn("Failed to execute statement due to exception" + exception);
//				}
//			}
//		}
//	}

//	private void handleStatementWithTerminatedQueries(Command command, CommandId commandId, Map<QueryId, CommandId> terminatedQueries, boolean wasDropped) throws Exception {		
//		try {
//		      String statementString = command.getStatement();
//		      commandStore.storeCommandStatus(commandId, CommandStatus.Status.PARSING, "Parsing statement");
//		      Statement statement = parseSingleStatement(statementString);
//		      commandStore.storeCommandStatus(commandId, CommandStatus.Status.EXECUTING, "Executing statement");
//		      executeStatement(statement, command, commandId, terminatedQueries, wasDropped);
//		    } catch (WakeupException exception) {
//		      throw exception;
//		    } catch (Exception exception) {
////		      log.error("Failed to handle: " + command, exception);
////		      CommandStatus errorStatus = new CommandStatus(CommandStatus.Status.ERROR, ExceptionUtil.stackTraceToString(exception));
////		      statusStore.put(commandId, errorStatus);
////		      completeStatusFuture(commandId, errorStatus);
//				String stackTraceString = ExceptionUtil.stackTraceToString(exception);
//				commandStore.storeCommandStatus(commandId, CommandStatus.Status.ERROR, stackTraceString);
//				commandStore.completeCommand(commandId, commandStore.getStatus(commandId));
//		    }
//	}

	private void executeStatement(Statement statement, Command command, CommandId commandId, Map<QueryId, CommandId> terminatedQueries, boolean wasDropped) throws Exception {
//		String statementStr = command.getStatement();
//
//		DDLCommandResult result = null;
//		String successMessage = "";
//
//		if ( statement instanceof RegisterTopic
//				|| statement instanceof CreateStream
//				|| statement instanceof CreateTable
//				|| statement instanceof DropTopic
//				|| statement instanceof DropStream
//				|| statement instanceof DropTable ) {
//			result = ksqlEngine.getQueryEngine().handleDdlStatement(statement, command.getStreamsProperties());
//		} else if ( statement instanceof CreateStreamAsSelect ) {
//			CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
//			QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
//			Query query = ksqlEngine.addInto(
//					createStreamAsSelect.getQuery(),
//					querySpecification,
//					createStreamAsSelect.getName().getSuffix(),
//					createStreamAsSelect.getProperties(),
//					createStreamAsSelect.getPartitionByColumn()
//					);
//			if ( startQuery(statementStr, query, commandId, terminatedQueries) ) {
//				successMessage = "Stream created and running";
//			} else {
//				return;
//			}
//		} else if ( statement instanceof CreateTableAsSelect ) {
//			CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
//			QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();
//			Query query = ksqlEngine.addInto(
//					createTableAsSelect.getQuery(),
//					querySpecification,
//					createTableAsSelect.getName().getSuffix(),
//					createTableAsSelect.getProperties(),
//					Optional.empty()
//					);
//			if ( startQuery(statementStr, query, commandId, terminatedQueries) ) {
//				successMessage = "Table created and running";
//			} else {
//				return;
//			}
//		} else if ( statement instanceof TerminateQuery ) {
//			terminateQuery((TerminateQuery) statement);
//			successMessage = "Query terminated.";
//		} else if ( statement instanceof RunScript ) {
//			if ( command.getStreamsProperties().containsKey(DdlConfig.SCHEMA_FILE_CONTENT_PROPERTY) ) {
//				String queries = (String) command.getStreamsProperties().get(DdlConfig.SCHEMA_FILE_CONTENT_PROPERTY);
//				List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(queries, command.getStreamsProperties());
//				for ( QueryMetadata queryMetadata : queryMetadataList ) {
//					if ( queryMetadata instanceof PersistentQueryMetadata ) {
//						PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
//						persistentQueryMetadata.getKafkaStreams().start();
//					}
//				}
//			} else {
//				throw new KsqlException("No statements received for LOAD FROM FILE.");
//			}
//		}else {
//			throw new Exception(String.format("Unexpected statement type: %s", statement.getClass().getName()));
//		}
//
//		CommandStatus successStatus = new CommandStatus(CommandStatus.Status.SUCCESS,
//				result != null ? result.getMessage(): successMessage);
//		commandStore.storeCommandStatus(commandId, CommandStatus.Status.SUCCESS,
//				result != null ? result.getMessage(): successMessage);
//		commandStore.completeCommand(commandId, commandStore.getStatus(commandId));
		
		String statementStr = command.getStatement();

	    DDLCommandResult result = null;
	    String successMessage = "";
	    if (statement instanceof DDLStatement) {
	      result =
	          ksqlEngine.executeDdlStatement(statementStr, (DDLStatement) statement, command.getStreamsProperties());
	    } else if (statement instanceof CreateAsSelect) {
	      successMessage = handleCreateAsSelect((CreateAsSelect)
	          statement,
	          command,
	          commandId,
	          terminatedQueries,
	          statementStr,
	          wasDropped);
	      if (successMessage == null) return;
	    } else if (statement instanceof TerminateQuery) {
	      terminateQuery((TerminateQuery) statement);
	      successMessage = "Query terminated.";
	    } else if (statement instanceof RunScript) {
	      handleRunScript(command);
	    }else {
	      throw new Exception(String.format(
	          "Unexpected statement type: %s",
	          statement.getClass().getName()
	      ));
	    }
	    // TODO: change to unified return message
	    CommandStatus successStatus = new CommandStatus(CommandStatus.Status.SUCCESS,
	        result != null ? result.getMessage(): successMessage);
	    statusStore.put(commandId, successStatus);
	    completeStatusFuture(commandId, successStatus);
	}

//	  private void handleRunScript(Command command) throws Exception {
//	    if (command.getStreamsProperties().containsKey(DdlConfig.SCHEMA_FILE_CONTENT_PROPERTY)) {
//	      String queries =
//	          (String) command.getStreamsProperties().get(DdlConfig.SCHEMA_FILE_CONTENT_PROPERTY);
//	      List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(queries,
//	                                          command.getStreamsProperties());
//	      for (QueryMetadata queryMetadata : queryMetadataList) {
//	        if (queryMetadata instanceof PersistentQueryMetadata) {
//	          PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
//	          persistentQueryMetadata.getKafkaStreams().start();
//	        }
//	      }
//	    } else {
//	      throw new KsqlException("No statements received for LOAD FROM FILE.");
//	    }
//	  }
//
//	  private String handleCreateAsSelect(final CreateAsSelect statement,
//	                                      final Command command,
//	                                      final CommandId commandId,
//	                                      final Map<QueryId, CommandId> terminatedQueries,
//	                                      final String statementStr,
//	                                      final boolean wasDropped) throws Exception {
//	    QuerySpecification querySpecification =
//	        (QuerySpecification) statement.getQuery().getQueryBody();
//	    Query query = ksqlEngine.addInto(
//	        statement.getQuery(),
//	        querySpecification,
//	        statement.getName().getSuffix(),
//	        statement.getProperties(),
//	        statement.getPartitionByColumn()
//	    );
//	    if (startQuery(statementStr, query, commandId, terminatedQueries, command, wasDropped)) {
//	      return statement instanceof CreateTableAsSelect
//	          ? "Table created and running"
//	          : "Stream created and running";
//	    }
//
//	    return null;
//	  }
	
	private String handleCreateAsSelect(final CreateAsSelect statement,
              final Command command,
              final CommandId commandId,
              final Map<QueryId, CommandId> terminatedQueries,
              final String statementStr,
              final boolean wasDropped) throws Exception {
		QuerySpecification querySpecification =
		(QuerySpecification) statement.getQuery().getQueryBody();
		Query query = ksqlEngine.addInto(
		statement.getQuery(),
		querySpecification,
		statement.getName().getSuffix(),
		statement.getProperties(),
		statement.getPartitionByColumn()
		);
		if (startQuery(statementStr, query, commandId, terminatedQueries, command, wasDropped)) {
			return statement instanceof CreateTableAsSelect
			? "Table created and running"
			: "Stream created and running";
		}

		return null;
	}
	
//	  private boolean startQuery(
//		      String queryString,
//		      Query query,
//		      CommandId commandId,
//		      Map<QueryId, CommandId> terminatedQueries,
//		      Command command,
//		      boolean wasDropped) throws Exception {
//		    if (query.getQueryBody() instanceof QuerySpecification) {
//		      QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
//		      Relation into = querySpecification.getInto();
//		      if (into instanceof Table) {
//		        Table table = (Table) into;
//		        if (ksqlEngine.getMetaStore().getSource(table.getName().getSuffix()) != null) {
//		          throw new Exception(String.format(
//		              "Sink specified in INTO clause already exists: %s",
//		              table.getName().getSuffix().toUpperCase()
//		          ));
//		        }
//		      }
//		    }
//
//		    QueryMetadata queryMetadata = ksqlEngine.buildMultipleQueries(
//		        queryString,
//		        command.getStreamsProperties()
//		    ).get(0);
//
//		    if (queryMetadata instanceof PersistentQueryMetadata) {
//		      PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
//		      final QueryId queryId = persistentQueryMetadata.getId();
//
//		      if (terminatedQueries != null && terminatedQueries.containsKey(queryId)) {
////		        CommandId terminateId = terminatedQueries.get(queryId);
////		        statusStore.put(
////		            terminateId,
////		            new CommandStatus(CommandStatus.Status.SUCCESS, "Termination request granted")
////		        );
////		        statusStore.put(
////		            commandId,
////		            new CommandStatus(CommandStatus.Status.TERMINATED, "Query terminated")
////		        );
//		        ksqlEngine.terminateQuery(queryId, false);
//		        return false;
//		      } else if (wasDropped){
//		        ksqlEngine.terminateQuery(queryId, false);
//		        return false;
//		      } else {
//		        persistentQueryMetadata.getKafkaStreams().start();
//		        return true;
//		      }
//
//		    } else {
//		      throw new Exception(String.format(
//		          "Unexpected query metadata type: %s; was expecting %s",
//		          queryMetadata.getClass().getCanonicalName(),
//		          PersistentQueryMetadata.class.getCanonicalName()
//		      ));
//		    }
//		  }

//	private void terminateQuery(TerminateQuery terminateQuery) throws Exception {
//		long queryId = terminateQuery.getQueryId();
//		QueryMetadata queryMetadata = ksqlEngine.getPersistentQueries().get(queryId);
//		if ( !ksqlEngine.terminateQuery(queryId, true) ) {
//			throw new Exception(String.format("No running query with id %d was found", queryId));
//		}
//
//		CommandId.Type commandType;
//		DataSource.DataSourceType sourceType = queryMetadata.getOutputNode().getTheSourceNode().getDataSourceType();
//		switch ( sourceType ) {
//		case KTABLE:
//			commandType = CommandId.Type.TABLE;
//			break;
//		case KSTREAM:
//			commandType = CommandId.Type.STREAM;
//			break;
//		default:
//			throw new
//			Exception(String.format("Unexpected source type for running query: %s", sourceType));
//		}
//
//		String queryEntity = ((KsqlStructuredDataOutputNode) queryMetadata.getOutputNode()).getKsqlTopic().getName();
//		//		    CommandId queryStatementId = new CommandId(commandType, queryEntity);
//		//		    statusStore.put(
//		//		        queryStatementId,
//		//		        new CommandStatus(CommandStatus.Status.TERMINATED, "Query terminated")
//		//		    );
//	}

	public Statement parseSingleStatement(String statementString) throws Exception {
		List<Statement> statements = ksqlEngine.getStatements(statementString);
		if ( statements == null ) {
			throw new IllegalArgumentException("Call to KsqlEngine.getStatements() returned null");
		} else if ( (statements.size() != 1) ) {
			throw new IllegalArgumentException(
					String.format("Expected exactly one KSQL statement; found %d instead", statements.size())
					);
		} else {
			return statements.get(0);
		}
	}

	private StreamList listStreams(String statementText) {
		return StreamList.fromKsqlStreams(statementText, getSpecificSources(KsqlStream.class));
	}

	private TableList listTables(String statementText) {
		return TableList.fromKsqlTables(statementText, getSpecificSources(KsqlTable.class));
	}

	private <S extends StructuredDataSource> List<S> getSpecificSources(Class<S> dataSourceClass) {
		return ksqlEngine.getMetaStore().getAllStructuredDataSources().values().stream()
				.filter(dataSourceClass::isInstance)
				//	        .filter(structuredDataSource -> !structuredDataSource.getName().equalsIgnoreCase(this.streamName))
				.map(dataSourceClass::cast)
				.collect(Collectors.toList());
	}
	
	private void executeDDLCommand(DDLCommand ddlCommand) {
	    DDLCommandResult ddlCommandResult = new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(ddlCommand);
	    if (!ddlCommandResult.isSuccess()) {
	      throw new KsqlException(ddlCommandResult.getMessage());
	    }
	  }
	  
	private Pair<AbstractStreamCreateStatement, String> maybeAddFieldsFromSchemaRegistry(
	      AbstractStreamCreateStatement streamCreateStatement,
	      Map<String, Object> streamsProperties
	  ) {
	    Pair<AbstractStreamCreateStatement, String> avroCheckResult =
	        new AvroUtil().checkAndSetAvroSchema(streamCreateStatement, streamsProperties,
	                                             ksqlEngine.getSchemaRegistryClient());
	    return avroCheckResult;
	  }
	
	void handleRestoration(final RestoreCommands restoreCommands) throws Exception {
	    restoreCommands.forEach(((commandId, command, terminatedQueries, wasDropped) -> {
	      LOGGER.info("Executing prior statement: '{}'", command);
	      try {
	        handleStatementWithTerminatedQueries(
	            command,
	            commandId,
	            terminatedQueries,
	            wasDropped);
	      } catch (Exception exception) {
	    	  LOGGER.warn("Failed to execute statement due to exception", exception);
	      }
	    }));
	  }

}


