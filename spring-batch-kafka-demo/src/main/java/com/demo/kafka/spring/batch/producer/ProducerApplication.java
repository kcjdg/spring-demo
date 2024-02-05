package com.demo.kafka.spring.batch.producer;


import com.demo.kafka.spring.batch.CustomerTx;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.JacksonUtils;

import javax.swing.text.MaskFormatter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;
import java.util.function.Function;


@EnableBatchProcessing
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@RequiredArgsConstructor
public class ProducerApplication {

    public static void main(String args[]) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final KafkaTemplate<Long, CustomerTx> template;

    @Value("record.csv")
    private Resource inputCsv;

    @Bean
    Job job() {
        return this.jobBuilderFactory
                .get("job")
                .start(start())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    KafkaItemWriter<Long, CustomerTx> kafkaItemWriter() {
        return new KafkaItemWriterBuilder<Long, CustomerTx>()
                .kafkaTemplate(template)
                .itemKeyMapper(CustomerTx::getId)
                .build();
    }

    public ItemReader<CustomerTx> itemReader()
            throws UnexpectedInputException, ParseException {
        FlatFileItemReader<CustomerTx> reader = new FlatFileItemReader<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] tokens = {"id", "name", "transactiondate", "amount", "accountNo"};
        tokenizer.setNames(tokens);
        reader.setResource(inputCsv);
        DefaultLineMapper<CustomerTx> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> new CustomerTx(
                fieldSet.readLong(0),
                fieldSet.readString(1),
                LocalDate.parse(fieldSet.readString(2), DateTimeFormatter.ofPattern("dd/MM/yyyy")),
                fieldSet.readDouble(3),
                fieldSet.readString(4)
        ));
        reader.setLineMapper(lineMapper);
        return reader;
    }

    public ItemProcessor<CustomerTx, CustomerTx> processor() {
        Function<String, String> mask = (str) -> str.replaceAll("(\\b\\w{1,3})\\w", "$1" + "*".repeat(3));
        return tx -> new CustomerTx(
                tx.getId(),
                mask.apply(tx.getName().toUpperCase()),
                tx.getTransactionDate(),
                tx.getTransactionAmount(),
                mask.apply(tx.getAccountNo()));
    }

    @Bean
    Step start() {
        return this.stepBuilderFactory
                .get("s1")
                .<CustomerTx, CustomerTx>chunk(10)
                .processor(processor())
                .reader(itemReader())
                .writer(kafkaItemWriter())
                .build();
    }

}
