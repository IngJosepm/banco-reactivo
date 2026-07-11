package com.ledger.stream;

import com.ledger.api.Dtos.AlertaFraude;
import com.ledger.api.Dtos.EventoStream;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class EventoBus {

    private final Sinks.Many<EventoStream> eventos =
            Sinks.many().multicast().onBackpressureBuffer();

    private final Sinks.Many<AlertaFraude> alertas =
            Sinks.many().multicast().onBackpressureBuffer();

    public void publicar(EventoStream evento) { eventos.tryEmitNext(evento); }
    public Flux<EventoStream> flujo() { return eventos.asFlux(); }

    public void publicarAlerta(AlertaFraude a) { alertas.tryEmitNext(a); }
    public Flux<AlertaFraude> alertas() { return alertas.asFlux(); }
}