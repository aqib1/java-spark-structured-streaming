package com.spark.kafka.task;

import com.spark.kafka.task.client.SSClient;

public class SparkKafkaApplication {

	public static void main(String[] args) {
		SSClient.getInstance().initSpark().loadDataFromKafka().writeData();
	}

}
