package com.ledger.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.api.Dtos;
import com.ledger.api.Dtos.TransferenciaResponse;
import com.ledger.domain.Cuenta;
import com.ledger.domain.EventoCuenta;
import com.ledger.kafka.EventoProductor;
import com.ledger.store.EventStore;
import com.ledger.store.IdempotenciaStore;
import com.ledger.stream.EventoBus;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class TransferenciaService {

    private final EventStore store;
    private final IdempotenciaStore idem;
    private final CuentaService cuentas;
    private final ObjectMapper mapper;
    private final EventoProductor productor;

    public TransferenciaService(EventStore store, IdempotenciaStore idem,
                                CuentaService cuentas, ObjectMapper mapper , EventoProductor productor) {
        this.store = store;
        this.idem = idem;
        this.cuentas = cuentas;
        this.mapper = mapper;
        this.productor = productor;
    }

    public Mono<TransferenciaResponse> transferir(String clave, UUID origenId, UUID destinoId,
                                                  BigDecimal monto, String concepto) {
        return idem.buscar(clave)
                .map(json -> leer(json).comoRepetida())
                .switchIfEmpty(Mono.defer(() -> ejecutar(clave, origenId, destinoId, monto, concepto)))
                .doOnSuccess(resp -> {
                    if (resp != null && !resp.repetida()) {
                        productor.publicar(new Dtos.EventoStream("TransferenciaEnviada", origenId,
                                null, monto, null, concepto, Instant.now()));
                        productor.publicar(new Dtos.EventoStream("TransferenciaRecibida", destinoId,
                                null, monto, null, concepto, Instant.now()));
                    }
                });
    }

    @Transactional
    protected Mono<TransferenciaResponse> ejecutar(String clave, UUID origenId, UUID destinoId,
                                                   BigDecimal monto, String concepto) {
        if (origenId.equals(destinoId))
            return Mono.error(new CuentaService.ReglaViolada("No puedes transferir a la misma cuenta"));
        if (monto == null || monto.signum() <= 0)
            return Mono.error(new CuentaService.ReglaViolada("El monto debe ser positivo"));

        String transferenciaId = UUID.randomUUID().toString();

        return Mono.zip(cuentas.cargar(origenId), cuentas.cargar(destinoId))
                .flatMap(par -> {
                    Cuenta origen = par.getT1();
                    Cuenta destino = par.getT2();

                    if (!origen.existe()) return Mono.error(new CuentaService.CuentaNoEncontrada(origenId));
                    if (!destino.existe()) return Mono.error(new CuentaService.CuentaNoEncontrada(destinoId));
                    if (origen.saldo().compareTo(monto) < 0)
                        return Mono.error(new CuentaService.ReglaViolada(
                                "Saldo insuficiente. Disponible: " + origen.saldo()));

                    var enviada = new EventoCuenta.TransferenciaEnviada(
                            origenId, destinoId, monto, concepto, transferenciaId);
                    var recibida = new EventoCuenta.TransferenciaRecibida(
                            destinoId, origenId, monto, concepto, transferenciaId);

                    // Ambos inserts, misma transacción. O los dos, o ninguno.
                    return store.agregar(origenId, origen.version(), List.of(enviada))
                            .then(store.agregar(destinoId, destino.version(), List.of(recibida)))
                            .then(Mono.just(new TransferenciaResponse(
                                    transferenciaId, origenId, destinoId, monto,
                                    origen.saldo().subtract(monto), false)));
                })
                // 3. Sellamos la clave. Si otra petición idéntica ganó la carrera,
                //    la PK revienta, la transacción hace rollback y devolvemos la suya.
                .flatMap(resp -> idem.guardar(clave, escribir(resp)).thenReturn(resp))
                .onErrorResume(DataIntegrityViolationException.class,
                        ex -> idem.buscar(clave).map(json -> leer(json).comoRepetida()));
    }

    private String escribir(TransferenciaResponse r) {
        try { return mapper.writeValueAsString(r); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }

    private TransferenciaResponse leer(String json) {
        try { return mapper.readValue(json, TransferenciaResponse.class); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
}