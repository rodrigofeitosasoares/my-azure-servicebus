package com.myazure.servicebus.lib;

import java.util.List;

import com.azure.messaging.servicebus.ServiceBusErrorContext;

public abstract class MyAzureSB {
	
	abstract void processarErro(ServiceBusErrorContext context);
	
	abstract void enviarMensagem(String mensagem);
	
	abstract void enviarMensagensEmLote(List<String> mensagem);
	
	abstract void receberMensagens() throws InterruptedException;
	
	abstract void receberMensagens(Integer tempoMaxConexaoAberta) throws InterruptedException;

}
