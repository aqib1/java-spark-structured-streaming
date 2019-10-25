package com.spark.kafka.task.exceptions;

public class SparkSessionFailoverException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5511413755856786260L;

	public SparkSessionFailoverException(String message) {
		super(message);
	}

	public SparkSessionFailoverException(String message, Throwable e) {
		super(message, e);
	}

}
