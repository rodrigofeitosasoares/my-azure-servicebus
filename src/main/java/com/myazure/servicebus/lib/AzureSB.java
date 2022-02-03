package com.myazure.servicebus.lib;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AzureSB {

	public static void sendQueueMessage(String conn, String queueName, String message) {
		if (conn == null) {
			throw new RuntimeException("Conexão para envio de mensagem para a fila " + queueName + " não configurada");
		}
			
	    ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .connectionString(conn)
	            .sender()
	            .queueName(queueName)
	            .buildClient();

	    senderClient.sendMessage(new ServiceBusMessage(message));
	    System.out.println("Mensagem enviada para a fila: " + queueName);
	}
	
	public static void sendTopicMessage(String conn, String topicName, String message) {
		if (conn == null) {
			throw new RuntimeException("Conexão para envio de mensagem para o tópico " + topicName + " não configurada");
		}
		
	    ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .connectionString(conn)
	            .sender()
	            .topicName(topicName)
	            .buildClient();

	    senderClient.sendMessage(new ServiceBusMessage(message));
	    System.out.println("Mensagem enviada para o topico: " + topicName);
	}

	public static void sendQueueMessageBatch(String conn, String queueName, List<String> messages) {
		if (conn == null) {
			throw new RuntimeException("Conexão para envio de mensagem para a fila " + queueName + " não configurada");
		}
		
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .connectionString(conn)
	            .sender()
	            .queueName(queueName)
	            .buildClient();
		
		sendMessageBatch(senderClient, conn, queueName, messages);
	}
	
	public static void sendTopicMessageBatch(String conn, String topicName, List<String> messages) {
		if (conn == null) {
			throw new RuntimeException("Conexão para envio de mensagem para o tópico " + topicName + " não configurada");
		}
		
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .connectionString(conn)
	            .sender()
	            .topicName(topicName)
	            .buildClient();
		
		sendMessageBatch(senderClient, conn, topicName, messages);
	}
	
	private static void sendMessageBatch(ServiceBusSenderClient senderClient, String conn, String topicName, List<String> messages) {
		
	    // Creates an ServiceBusMessageBatch where the ServiceBus.
	    ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();
	    
	    // We try to add as many messages as a batch can fit based on the maximum size and send to Service Bus when
	    // the batch can hold no more messages. Create a new batch for next set of messages and repeat until all
	    // messages are sent.        
	    for (String m : messages) {
	    	
	    	ServiceBusMessage message = new ServiceBusMessage(m);
	    	
	        if (messageBatch.tryAddMessage(message)) {
	            continue;
	        }

	        // The batch is full, so we create a new batch and send the batch.
	        senderClient.sendMessages(messageBatch);

	        // create a new batch
	        messageBatch = senderClient.createMessageBatch();

	        // Add that message that we couldn't before.
	        if (!messageBatch.tryAddMessage(message)) {
	            System.err.printf("Message is too large for an empty batch. Skipping. Max size: %s.", messageBatch.getMaxSizeInBytes());
	        }
	    }

	    if (messageBatch.getCount() > 0) {
	        senderClient.sendMessages(messageBatch);
	        System.out.println("Sent a batch of messages to the topic: " + topicName);
	    }

	    //close the client
	    senderClient.close();
	}
	
	public static void listenQueue(String conn, String queueName, Consumer<ServiceBusReceivedMessageContext> successCallback, 
			Consumer<ServiceBusErrorContext> errorCallback) throws InterruptedException {
		listenQueue(conn, queueName, successCallback, errorCallback, null);
	}
	
	public static void listenQueue(String conn, String queueName, Consumer<ServiceBusReceivedMessageContext> successCallback, 
			Consumer<ServiceBusErrorContext> errorCallback, Integer executionTimeInSeconds) throws InterruptedException {
		
		if (conn == null) {
			throw new RuntimeException("Conexão para recebimento de mensagens da fila " + queueName + " não configurada");
		}
		
		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
	        .connectionString(conn)
	        .processor()
	        .queueName(queueName)
	        .processMessage(successCallback)
	        .processError(errorCallback)
	        .buildProcessorClient();

	    processorClient.start();
	    
	    if (executionTimeInSeconds != null) {
	    	processorClient.stop();
	    }
	}
	
	public static void listenTopic(String conn, String topicName, String subscription, 
			Consumer<ServiceBusReceivedMessageContext> successCallback, Consumer<ServiceBusErrorContext> errorCallback) 
					throws InterruptedException {
		listenTopic(conn, topicName, subscription, successCallback, errorCallback, null);
	}
	
	public static void listenTopic(String conn, String topicName, String subscription, 
			Consumer<ServiceBusReceivedMessageContext> successCallback, Consumer<ServiceBusErrorContext> errorCallback, 
			Integer executionTimeInSeconds) throws InterruptedException {
		
		if (conn == null) {
			throw new RuntimeException("Conexão para recebimento de mensagens do tópico " + topicName + " não configurada");
		}
		
		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
	        .connectionString(conn)
	        .processor()
	        .topicName(topicName)
	        .subscriptionName(subscription)
	        .processMessage(successCallback)
	        .processError(errorCallback)
	        .buildProcessorClient();

	    processorClient.start();
	    
	    if (executionTimeInSeconds != null) {
	    	processorClient.stop();
	    }
	}
	
	public static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
	    System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n", context.getFullyQualifiedNamespace(), context.getEntityPath());

	    if (!(context.getException() instanceof ServiceBusException)) {
	        System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
	        return;
	    }

	    ServiceBusException exception = (ServiceBusException) context.getException();
	    ServiceBusFailureReason reason = exception.getReason();

	    if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
	        || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
	        || reason == ServiceBusFailureReason.UNAUTHORIZED) {
	        System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n", reason, exception.getMessage());

	        countdownLatch.countDown();
	    } else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
	        System.out.printf("Message lock lost for message: %s%n", context.getException());
	    } else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
	        try {
	            // Choosing an arbitrary amount of time to wait until trying again.
	            TimeUnit.SECONDS.sleep(1);
	        } catch (InterruptedException e) {
	            System.err.println("Unable to sleep for period of time");
	        }
	    } else {
	        System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(), reason, context.getException());
	    }
	}
	
}
