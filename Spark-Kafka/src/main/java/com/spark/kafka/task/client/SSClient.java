package com.spark.kafka.task.client;

import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.spark.kafka.task.exceptions.DataSetFailoverException;
import com.spark.kafka.task.exceptions.SparkSessionFailoverException;

/**
 * @author Shahzad Masud
 *
 *         In this example we are going to use spark structured streaming, The
 *         difference between spark streaming and spark structured streaming is
 *         that structured streaming does not use any concept of micro-batches
 *         like spark streaming, instead it's architecture is more likely
 *         towards real streaming where data is poll after some
 *         duration/interval and result is appended in a unbounded table.
 * 
 *         where as spark streaming use a concept of batches where record
 *         belongs to a batch of DStream
 *
 */
public class SSClient {

	private static final String KAFKA_FORMAT = "kafka";
	private static final String SUBSCRIBER_KEY = "subscribe";
	private static String TOPIC_NAME = "MTKAF";
	private static final String KAFKA_BOOTSTRAP_SERVER_KEY = "kafka.bootstrap.servers";
	private static final int BROKER_PORT_NUMBER = 6667;
	private static final String BROKER_DOMAIN_NAME = "sandbox-hdp.hortonworks.com";
	private static final String SPAKR_STREAM_STARTING_OFFSET_KEY = "startingOffsets";
	private static final String SPARK_STREAM_STARTING_OFFSET_BEGINNING = "earliest";

	private static final String MASTER_PATH = "local[*]";
	private static final String SPARK_SQL_STREAMING_CHECKPOINT_LOCATION_CONFIG = "spark.sql.streaming.checkpointLocation";
	private static final String SPARK_SQL_STREAMING_CHECKPOINT_LOCATION = "/user/sparktest/checkpoints";
	private static SSClient ssClient = null;
	private SparkSession sparkSession = null;
	private Dataset<Row> datasets;

	public SSClient initSpark() {
		try {
			sparkSession = SparkSession.builder().appName(SSClient.class.getName()).master(MASTER_PATH)
					.config(SPARK_SQL_STREAMING_CHECKPOINT_LOCATION_CONFIG, SPARK_SQL_STREAMING_CHECKPOINT_LOCATION)
					.getOrCreate();
			
		} catch (Exception e) {
			throw new SparkSessionFailoverException(e.getMessage(), e);
		}
		return ssClient;
	}

	public SSClient loadDataFromKafka() {
		try {
			datasets = sparkSession.readStream().format(KAFKA_FORMAT)
					.option(KAFKA_BOOTSTRAP_SERVER_KEY, BROKER_DOMAIN_NAME + ":" + BROKER_PORT_NUMBER)
					.option(SUBSCRIBER_KEY, TOPIC_NAME)
					.option(SPAKR_STREAM_STARTING_OFFSET_KEY, SPARK_STREAM_STARTING_OFFSET_BEGINNING).load();
		} catch (Exception e) {
			throw new DataSetFailoverException(e.getMessage(), e);
		}
		return ssClient;
	}
	
	public void writeData() {
		datasets = datasets.selectExpr("CAST(value AS STRING)");
		Dataset<String> dataAsJson = datasets.toJSON();
		StreamingQuery query = dataAsJson.writeStream()
		  .format("console")
		  .outputMode("complete")
		  .start();
		
		query.awaitTermination();
	}

	private SSClient() {

	}

	public static SSClient getInstance() {
		if (Objects.isNull(ssClient)) {
			synchronized (SSClient.class) {
				if (Objects.isNull(ssClient)) {
					ssClient = new SSClient();
				}
			}
		}
		return ssClient;
	}
}
