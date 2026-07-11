package com.ledger.stream;

import com.ledger.api.Dtos;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("cloud")
public class PublicadorLocal implements PublicadorEventos{

    private final EventoBus bus;

    public PublicadorLocal(EventoBus bus) {
        this.bus = bus;
    }
    @Override
    public void publicar(Dtos.EventoStream evento){
        bus.publicar(evento);
    }

}
