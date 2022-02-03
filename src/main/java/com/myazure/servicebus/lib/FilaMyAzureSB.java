package com.myazure.servicebus.lib;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class FilaMyAzureSB extends MyAzureSB {

	private final String name;
	private final String senderConnection;
	private final String listenerConnection;
	
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
	 * Envia mensagem para a fila
	 * @param mensagem
	 */
	@Override
	public void enviarMensagem(String mensagem) {
		AzureSB.sendQueueMessage(senderConnection, name, mensagem);
	}
	
	/**
	 * Envia mensagens em lote para a fila
	 * @param mensagens
	 */
	@Override
	public void enviarMensagensEmLote(List<String> mensagens) {
		AzureSB.sendQueueMessageBatch(senderConnection, name, mensagens);
	}

	/**
	 * Inicia o recebimento de mensagens da fila
	 */
	@Override
	public void receberMensagens() throws InterruptedException {
		AzureSB.listenQueue(listenerConnection, name, this::processarMensagem, this::processarErro);
	}
	
	/**
	 * Inicia o recebimento de mensagens da fila durante um tempo máximo
	 * @param tempoExecucaoEmSegundos
	 */
	@Override
	public void receberMensagens(Integer tempoExecucaoEmSegundos) throws InterruptedException {
		AzureSB.listenQueue(listenerConnection, name, this::processarMensagem, this::processarErro, tempoExecucaoEmSegundos);
	}

}
