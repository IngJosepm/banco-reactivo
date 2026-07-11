package com.ledger.api;

import com.ledger.api.Dtos.EventoStream;
import com.ledger.stream.EventoBus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
public class StreamController {

    private final EventoBus bus;

    public StreamController(EventoBus bus) { this.bus = bus; }

    @GetMapping(value = "/api/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<EventoStream> stream() {
        return bus.flujo()
                .mergeWith(Flux.interval(Duration.ofSeconds(20))
                        .map(t -> EventoStream.latido()));
    }

    @GetMapping(value = "/api/alertas", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Dtos.AlertaFraude> alertas() {
        return bus.alertas();
    }
}