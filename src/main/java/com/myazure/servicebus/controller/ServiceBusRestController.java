package com.myazure.servicebus.controller;

import java.util.List;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.myazure.servicebus.service.ExemploFilaService;
import com.myazure.servicebus.service.ExemploTopicoService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class ServiceBusRestController {

    private final ExemploTopicoService topico;
    private final ExemploFilaService fila;

    @PostMapping("/servicebus/v1/topico-teste")
    public void postTopicoTesteRodrigo(@RequestBody(required = false) String mensagem) {
        topico.enviarMensagem(mensagem);
    }
    
    @PostMapping("/servicebus/v1/topico-teste/em-lote")
    public void postTopicoTesteRodrigo(@RequestBody(required = false) List<String> mensagens) {
        topico.enviarMensagensEmLote(mensagens);
    }
    
    @PostMapping("/servicebus/v1/fila-teste")
    public void postFilaNotificacoes(@RequestBody(required = false) String mensagem) {
        fila.enviarMensagem(mensagem);
    }
    
    @PostMapping("/servicebus/v1/fila-teste/em-lote")
    public void postFilaNotificacoes(@RequestBody(required = false) List<String> mensagens) {
        fila.enviarMensagensEmLote(mensagens);
    }
    
}