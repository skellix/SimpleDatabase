package com.skellix.database.table.query.exception;

public class QueryParseException extends Exception {

	public QueryParseException(String errorString) {
		
		super(errorString);
	}
	
	public static QueryParseException format(String format, Object ... args) {
		
		return new QueryParseException(String.format(format, args));
	}
	
}
