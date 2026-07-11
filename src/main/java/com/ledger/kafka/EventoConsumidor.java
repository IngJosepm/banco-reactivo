package com.ledger.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.api.Dtos.EventoStream;
import com.ledger.stream.EventoBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("local")
public class EventoConsumidor {

    private static final Logger log = LoggerFactory.getLogger(EventoConsumidor.class);

    private final EventoBus bus;
    private final ObjectMapper mapper;

    public EventoConsumidor(EventoBus bus, ObjectMapper mapper) {
        this.bus = bus;
        this.mapper = mapper;
    }

    @KafkaListener(topics = EventoProductor.TOPIC)
    public void consumir(String mensaje) {
        try {
            EventoStream evento = mapper.readValue(mensaje, EventoStream.class);
            bus.publicar(evento);
        } catch (Exception e) {
            log.error("Mensaje ilegible en Kafka: {}", mensaje, e);
        }
    }
}