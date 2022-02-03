package com.myazure.servicebus.lib;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class TopicoMyAzureSB extends MyAzureSB {

	private final String name;
	private final String senderConnection;
	private final String listenerConnection;
	private final String subscription;
	
	/**
	 * Processa a mensagem recebida
	 * @param context
	 */
	protected abstract void processarMensagem(ServiceBusReceivedMessageContext context);
	
	/**
	 * Processa erro durante o recebimento de uma mensagem
	 * @param context
	 * @param countdownLatch
	 */
	protected void processarErro(ServiceBusErrorContext context) {
		AzureSB.processError(context, new CountDownLatch(1));
	}
	
	/**
	 * Envia mensagem para o tópico
	 * @param mensagem
	 */
	@Override
	public void enviarMensagem(String mensagem) {
		AzureSB.sendTopicMessage(senderConnection, name, mensagem);
	}
	
	/**
	 * Envia mensagens em lote para o tópico
	 * @param mensagens
	 */
	@Override
	public void enviarMensagensEmLote(List<String> mensagens) {
		AzureSB.sendTopicMessageBatch(senderConnection, name, mensagens);
	}
	
	/**
	 * Inicia o recebimento de mensagens do tópico na respectiva subscrição
	 */
	@Override
	public void receberMensagens() throws InterruptedException {
		AzureSB.listenTopic(listenerConnection, name, subscription, this::processarMensagem, this::processarErro);
	}
	
	/**
	 * Inicia o recebimento de mensagens do tópico na respectiva subscrição durante um tempo máximo
	 * @param tempoExecucaoEmSegundos
	 */
	@Override
	public void receberMensagens(Integer tempoExecucaoEmSegundos) throws InterruptedException {
		AzureSB.listenTopic(listenerConnection, name, subscription, this::processarMensagem, this::processarErro, tempoExecucaoEmSegundos);
	}
	
}
