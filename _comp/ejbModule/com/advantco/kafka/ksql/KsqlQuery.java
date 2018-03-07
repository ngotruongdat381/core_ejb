package com.advantco.kafka.ksql;

import com.advantco.base.StringUtil;
import com.advantco.kafka.builder.KConfigContext;

public class KsqlQuery {
	private static final int BUF_SIZE = 2048;
	private static final String SPACE_CHAR = " ";
	private static final String EMPTY_STRING = "";
	private static final String WINDOW_TUMBLING = "WINDOW TUMBLING";
	private static final String WINDOW_HOPPING = "WINDOW HOPPING";
	private static final String WINDOW_SESSION = "WINDOW SESSION";
	private static final String SELECT = "SELECT";
	private static final String FROM = "FROM";
	private static final String WHERE = "WHERE";
	private static final String GROUP_BY = "GROUP BY";
	private static final String HAVING = "HAVING";
	
	private String applicationID;
	private String retries;
	private String maxPollRecords;
	
	private String typeOfCommand;
	private String nameCreate;
	private String colNameAndDataType;
	private String kafkaTopic;
	private String valueFormat;
	
	private String selectCommand;
	private String fromCommand;
	
	private boolean useWindowCommand;
	private String windowType;
	private String windowExpression;
	
	private String whereCommand;
	
	private boolean useGroupByCommand;
	private String groupByType;
	private String groupByExpression;
	
	private boolean useHavingCommand;
	private String havingExpression;
	
	private static KsqlQuery instance;
	 
	public static synchronized KsqlQuery getInstance(){
        if(instance == null){
            instance = new KsqlQuery();
        }
        return instance;
    }
	
	public String getApplicationID() {
		return applicationID;
	}

	public void setApplicationID(String applicationID) throws Exception {
		if ( applicationID.isEmpty() ) {
			throw new Exception("Application ID is empty");
		}
		this.applicationID = applicationID;
	}
	
	public String getRetries() {
		return retries;
	}

	public void setRetries(String retries) {
		if (KConfigContext.isValidNumericValue(retries)) {
			this.retries = retries;
		} else {
			this.retries = "0";
		}
	}
	
	public String getMaxPollRecords() {
		return maxPollRecords;
	}

	public void setMaxPollRecords(String maxPollRecords) {
		if (KConfigContext.isValidNumericValue(maxPollRecords)) {
			this.maxPollRecords = maxPollRecords;
		} else {
			this.maxPollRecords = "500";
		}
	}
	
	public String getTypeOfCommand() {
		return typeOfCommand;
	}

	public void setTypeOfCommand(String typeOfCommand) throws Exception {
		if ( typeOfCommand.isEmpty() ) {
			throw new Exception("Type of command is empty");
		}
		this.typeOfCommand = typeOfCommand;	
	}

	public String getNameCreate() {
		return nameCreate;
	}

	public void setNameCreate(String queryCommand) throws Exception {
		if ( queryCommand.isEmpty() ) {
			throw new Exception("Query command is empty");
		} 
		this.nameCreate = queryCommand;
	}

	public String getColNameAndDataType() {
		return colNameAndDataType;
	}

	public void setColNameAndDataType(String colNameAndDataType) throws Exception {	
		if ( colNameAndDataType.isEmpty() ) {
			throw new Exception("Column Name and Data Type is empty");
		}
		this.colNameAndDataType = colNameAndDataType;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) throws Exception {	
		if ( kafkaTopic.isEmpty() ) {
			throw new Exception("Column Name and Data Type is empty");
		}
		this.kafkaTopic = kafkaTopic;
	}

	public String getValueFormat() {
		return valueFormat;
	}

	public void setValueFormat(String valueFormat) throws Exception {
		if ( valueFormat.isEmpty() ) {
			throw new Exception("Value Format is empty");
		} 
		this.valueFormat = valueFormat;
	}

	public String getSelectCommand() {
		return selectCommand;
	}

	public void setSelectCommand(String selectCommand) throws Exception {
		if ( selectCommand.isEmpty() ) {
			throw new Exception("Select Command is empty");
		}
		this.selectCommand = selectCommand;
	}

	public String getFromCommand() {
		return fromCommand;
	}

	public void setFromCommand(String fromCommand) throws Exception {
		if ( fromCommand.isEmpty() ) {
			throw new Exception("Select Command is empty");
		}
		this.fromCommand = fromCommand;
	}
	
	public void setUseWindowCommand(boolean useWindowCommand) {
		this.useWindowCommand = useWindowCommand;
	}

	public String getWindowType() {
		return windowType;
	}

	public void setWindowType(String windowType) {
		this.windowType = windowType;
	}

	public String getWindowExpression() {
		return windowExpression;
	}

	public void setWindowExpression(String windowExpression) {
		this.windowExpression = windowExpression;
	}

	public String getWhereCommand() {
		return whereCommand;
	}

	public void setWhereCommand(String whereCommand) {
		this.whereCommand = whereCommand;
	}

	public void setUseGroupByCommand(boolean useGroupByCommand) {
		this.useGroupByCommand = useWindowCommand;
	}
	
	public String getGroupByType() {
		return groupByType;
	}

	public void setGroupByType(String groupByType) {
		this.groupByType = groupByType;
	}

	public String getGroupByExpression() {
		return groupByExpression;
	}

	public void setGroupByExpression(String groupByExpression) {
		this.groupByExpression = groupByExpression;
	}

	public void setUseHavingCommand(boolean useHavingCommand) {
		this.useHavingCommand = useHavingCommand;
	}
	
	public String getHavingExpression() {
		return havingExpression;
	}

	public void setHavingExpression(String havingExpression) {
		this.havingExpression = havingExpression;
	}
	
	public String buildCreateCommand() {
		if ( StringUtil.nullOrBlank(typeOfCommand) && StringUtil.nullOrBlank(nameCreate) && StringUtil.nullOrBlank(colNameAndDataType) && 
				StringUtil.nullOrBlank(kafkaTopic) && StringUtil.nullOrBlank(valueFormat)) {
			return EMPTY_STRING;
		}
		
		StringBuilder sBuilder = new StringBuilder(BUF_SIZE);
		sBuilder.append("CREATE").append(SPACE_CHAR).append(typeOfCommand).append(SPACE_CHAR).append(nameCreate).append(SPACE_CHAR);
		sBuilder.append("(").append(colNameAndDataType).append(")").append(SPACE_CHAR);
		sBuilder.append("WITH").append(SPACE_CHAR).append("(").append("kafka_topic='").append(kafkaTopic).append("',").append("value_format='").append(valueFormat).append("');");
		return sBuilder.toString();
	}
	
	public String buildSelectCommand() {
		if ( StringUtil.nullOrBlank(selectCommand) && StringUtil.nullOrBlank(fromCommand) ) {
			return EMPTY_STRING;
		}
		
		StringBuilder sBuilder = new StringBuilder(BUF_SIZE);
		sBuilder.append(SELECT).append(SPACE_CHAR).append(selectCommand).append(SPACE_CHAR);
		sBuilder.append(FROM).append(SPACE_CHAR).append(fromCommand);
		
		if ( useWindowCommand ) {
			if ( windowType.equals("tumbling") ) {
				sBuilder.append(SPACE_CHAR).append(WINDOW_TUMBLING).append(SPACE_CHAR).append(windowExpression);
			} else if ( windowType.equals("hopping") ) {
				sBuilder.append(SPACE_CHAR).append(WINDOW_HOPPING).append(SPACE_CHAR).append(windowExpression);
			} else if ( windowType.equals("session") ) {
				sBuilder.append(SPACE_CHAR).append(WINDOW_SESSION).append(SPACE_CHAR).append(windowExpression);
			}
		}
		
		if ( !whereCommand.isEmpty() ) {
			sBuilder.append(SPACE_CHAR).append(WHERE).append(SPACE_CHAR).append(whereCommand);
		}
		
		if ( useGroupByCommand ) {
			if ( groupByType.equals("groupBy") ) {
				sBuilder.append(SPACE_CHAR).append(GROUP_BY).append(SPACE_CHAR).append(groupByExpression);
			} else {
				// Do nothing
			}
		}
		
		if ( useHavingCommand && (!havingExpression.isEmpty()) ) {
			sBuilder.append(SPACE_CHAR).append(HAVING).append(SPACE_CHAR).append(havingExpression);
		}
		sBuilder.append(";");
		return sBuilder.toString();
	}
}
