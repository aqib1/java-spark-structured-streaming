package com.spark.kafka.task.exceptions;

public class DataSetFailoverException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2009928124293947716L;
	
	public DataSetFailoverException(String message) {
		super(message);
	}

	public DataSetFailoverException(String message, Throwable e) {
		super(message, e);
	}


}
