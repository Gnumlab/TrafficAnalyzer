package com.bridgestone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		System.setProperty("kafka.topic.helloworld", "helloworld.t");
		SpringApplication.run(KafkaApplication.class, args);
	}
}
