package com.myazure.servicebus.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.myazure.servicebus.lib.FilaMyAzureSB;

@Service
public class ExemploFilaService extends FilaMyAzureSB {

	public ExemploFilaService(@Value("${servicebus.fila-teste.name}") String name,
						      @Value("${servicebus.fila-teste.send.conn}") String senderConnection, 
							  @Value("${servicebus.fila-teste.listen.conn}") String listenerConnection) {
		super(name, senderConnection, listenerConnection);
	}
	
	@Override
	protected void processarMensagem(ServiceBusReceivedMessageContext context) {
		System.out.println("Mensagem recebida da fila: " + context.getMessage().getBody());
	}
	
}
