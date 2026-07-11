package com.ledger.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.api.Dtos.EventoStream;
import com.ledger.stream.PublicadorEventos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Profile("local")
public class EventoProductor implements PublicadorEventos {

    private static final Logger log = LoggerFactory.getLogger(EventoProductor.class);
    public static final String TOPIC = "eventos-cuenta";

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    public EventoProductor(KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
        this.kafka = kafka;
        this.mapper = mapper;
    }

    @Override
    public void publicar(EventoStream evento) {
        try {
            String clave = evento.cuentaId() != null ? evento.cuentaId().toString() : "sistema";
            kafka.send(TOPIC, clave, mapper.writeValueAsString(evento));
        } catch (Exception e) {
            log.error("No se pudo publicar en Kafka", e);
        }
    }
}