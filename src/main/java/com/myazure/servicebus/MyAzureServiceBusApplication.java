package com.myazure.servicebus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.myazure.servicebus.service.ExemploFilaService;
import com.myazure.servicebus.service.ExemploTopicoService;

@SpringBootApplication
public class MyAzureServiceBusApplication implements CommandLineRunner {
	
	@Autowired
	ExemploTopicoService topicoExemplo;
	
	@Autowired
	ExemploFilaService filaExemplo;
	
	public static void main(String[] args) {
		SpringApplication.run(MyAzureServiceBusApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		filaExemplo.receberMensagens();
		topicoExemplo.receberMensagens();
	}

}
