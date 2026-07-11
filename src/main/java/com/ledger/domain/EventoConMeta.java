package com.ledger.domain;

import java.time.Instant;

public record EventoConMeta(long version , Instant ocurridoEn, EventoCuenta evento) {


}
