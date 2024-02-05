package com.demo.kafka.spring.batch.consumer;

import com.demo.kafka.spring.batch.CustomerTx;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Properties;

@Log4j2
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
@RequiredArgsConstructor
@EnableBatchProcessing
public class ConsumerApplication {

	public static void main(String args[]) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	private final KafkaProperties properties;
	private final StepBuilderFactory stepBuilderFactory;
	private final JobBuilderFactory jobBuilderFactory;

	@Bean
	Job job() {
		return jobBuilderFactory.get("job")
			.incrementer(new RunIdIncrementer())
			.start(start())
			.build();

	}

	@Bean
	KafkaItemReader<Long, CustomerTx> kafkaItemReader() {
		var props = new Properties();
		props.putAll(this.properties.buildConsumerProperties());

		return new KafkaItemReaderBuilder<Long, CustomerTx>()
			.partitions(0)
			.consumerProperties(props)
			.name("customers-reader")
			.saveState(true)
			.topic("customers-trx-history")
			.build();
	}

	@Bean
	Step start() {
		var writer = new ItemWriter<CustomerTx>() {
			@Override
			public void write(List<? extends CustomerTx> items) throws Exception {
				items.forEach(it -> log.info("new customer transactions: " + it));
			}
		};
		return stepBuilderFactory
			.get("step")
			.<CustomerTx, CustomerTx>chunk(10)
			.writer(writer)
			.reader(kafkaItemReader())
			.build();
	}
}
