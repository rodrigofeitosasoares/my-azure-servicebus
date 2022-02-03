package com.myazure.servicebus.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.myazure.servicebus.lib.TopicoMyAzureSB;

@Service
public class ExemploTopicoService extends TopicoMyAzureSB {
	
	public ExemploTopicoService(@Value("${servicebus.topico-teste.name}") String name,
			                    @Value("${servicebus.topico-teste.send.conn}") String senderConnection, 
							    @Value("${servicebus.topico-teste.listen.conn}") String listenerConnection, 
							    @Value("${servicebus.topico-teste.listen.subscription}") String subscription) {
		super(name, senderConnection, listenerConnection, subscription);
	}
	
	@Override
	protected void processarMensagem(ServiceBusReceivedMessageContext context) {
		System.out.println("Mensagem recebida do tópico: " + context.getMessage().getBody());
	}
	
}
