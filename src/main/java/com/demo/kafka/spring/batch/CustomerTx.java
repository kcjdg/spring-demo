package com.demo.kafka.spring.batch;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.function.BiFunction;
import java.util.function.Function;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerTx {
	private Long id;
	private String name;
	@JsonDeserialize(using = LocalDateDeserializer.class)
	@JsonSerialize(using = LocalDateSerializer.class)
	private LocalDate transactionDate;
	private Double transactionAmount;
	private String accountNo;

}
