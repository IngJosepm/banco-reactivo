package com.ledger.stream;

import com.ledger.api.Dtos.EventoStream;

public interface PublicadorEventos {
    void publicar(EventoStream evento);
}