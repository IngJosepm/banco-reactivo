package com.ledger.service;

import com.ledger.api.Dtos;
import com.ledger.domain.Cuenta;
import com.ledger.domain.EventoConMeta;
import com.ledger.domain.EventoCuenta;
import com.ledger.kafka.EventoProductor;
import com.ledger.store.EventStore;
import com.ledger.stream.EventoBus;
import com.ledger.stream.PublicadorEventos;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class CuentaService {

    private final EventStore store;
    private final PublicadorEventos productor;


    public CuentaService(EventStore store, PublicadorEventos productor) {
        this.store = store;
        this.productor = productor;
    }


    public Mono<Cuenta> cargar(UUID id) {
        return store.leer(id).reduce(Cuenta.VACIA, Cuenta::aplicar);
    }


    public Mono<Cuenta> cargarEn(UUID id, Instant momento) {
        return store.leerHasta(id, momento).reduce(Cuenta.VACIA, Cuenta::aplicar);
    }

    public Flux<EventoConMeta> historial(UUID id) {
        return store.leer(id);
    }

    @Transactional
    public Mono<Cuenta> abrir(String titular) {
        UUID id = UUID.randomUUID();
        var evento = new EventoCuenta.CuentaAbierta(id, titular);
        return store.agregar(id, 0L, List.of(evento)).then(cargar(id));
    }

    @Transactional
    public Mono<Cuenta> depositar(UUID id, BigDecimal monto, String concepto) {
        return cargar(id).flatMap(c -> {
            if (!c.existe()) return Mono.error(new CuentaNoEncontrada(id));
            if (monto.signum() <= 0) return Mono.error(new ReglaViolada("El monto debe ser positivo"));
            var evento = new EventoCuenta.DineroDepositado(id, monto, concepto);
            return store.agregar(id, c.version(), List.of(evento))
                    .then(cargar(id))
                    .doOnNext(cuenta -> productor.publicar(new Dtos.EventoStream(
                            "DineroDepositado", id, cuenta.titular(),
                            monto, cuenta.saldo(), concepto, Instant.now())));

        });
    }

    @Transactional
    public Mono<Cuenta> retirar(UUID id, BigDecimal monto, String concepto) {
        return cargar(id).flatMap(c -> {
            if (!c.existe()) return Mono.error(new CuentaNoEncontrada(id));
            if (monto.signum() <= 0) return Mono.error(new ReglaViolada("El monto debe ser positivo"));
            if (c.saldo().compareTo(monto) < 0)
                return Mono.error(new ReglaViolada("Saldo insuficiente. Disponible: " + c.saldo()));
            var evento = new EventoCuenta.DineroRetirado(id, monto, concepto);
            return store.agregar(id, c.version(), List.of(evento))
                    .then(cargar(id))
                    .doOnNext(cuenta -> productor.publicar(new Dtos.EventoStream(
                            "DineroRetirado", id, cuenta.titular(),
                            monto, cuenta.saldo(), concepto, Instant.now())));
        });
    }

    public static class CuentaNoEncontrada extends RuntimeException {
        public CuentaNoEncontrada(UUID id) {
            super("Cuenta no encontrada: " + id);
        }
    }

    public Flux<Cuenta> listar() {
        return store.idsDeCuentas().flatMap(this::cargar).filter(Cuenta::existe);
    }

    public static class ReglaViolada extends RuntimeException {
        public ReglaViolada(String m) {
            super(m);
        }
    }
}