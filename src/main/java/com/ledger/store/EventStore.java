package com.ledger.store;

import com.ledger.domain.EventoConMeta;
import com.ledger.domain.EventoCuenta;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public class EventStore {

    private final DatabaseClient db;
    private final EventoJson json;

    public EventStore(DatabaseClient db, EventoJson json) {
        this.db = db;
        this.json = json;
    }


    public Mono<Void> agregar(UUID streamId, long versionEsperada, List<EventoCuenta> eventos) {
        return Flux.fromIterable(eventos)
                .index()
                .concatMap(t -> insertar(streamId, versionEsperada + t.getT1() + 1, t.getT2()))
                .onErrorMap(DataIntegrityViolationException.class,
                        ex -> new ConflictoDeConcurrencia(
                                "La cuenta cambió mientras procesábamos. Reintenta."))
                .then();
    }

    private Mono<Long> insertar(UUID streamId, long version, EventoCuenta e) {
        return db.sql("""
                    INSERT INTO eventos (stream_id, version, tipo, payload)
                    VALUES (:stream, :version, :tipo, :payload)
                    """)
                .bind("stream", streamId)
                .bind("version", version)
                .bind("tipo", e.getClass().getSimpleName())
                .bind("payload", json.escribir(e))
                .fetch()
                .rowsUpdated();
    }

    public Flux<EventoConMeta> leer(UUID streamId) {
        return db.sql("""
                    SELECT version, tipo, payload, ocurrido_en
                    FROM eventos WHERE stream_id = :stream ORDER BY version
                    """)
                .bind("stream", streamId)
                .map(this::mapear)
                .all();
    }

    /** Los ids de todas las cuentas: los streams que empezaron con CuentaAbierta. */
    public Flux<UUID> idsDeCuentas() {
        return db.sql("SELECT stream_id FROM eventos WHERE tipo = 'CuentaAbierta' ORDER BY id")
                .map(row -> row.get("stream_id", UUID.class))
                .all();
    }

    /** El historial hasta un instante. Esto es el viaje en el tiempo. */
    public Flux<EventoConMeta> leerHasta(UUID streamId, Instant hasta) {
        return db.sql("""
                    SELECT version, tipo, payload, ocurrido_en
                    FROM eventos
                    WHERE stream_id = :stream AND ocurrido_en <= :hasta
                    ORDER BY version
                    """)
                .bind("stream", streamId)
                .bind("hasta", OffsetDateTime.ofInstant(hasta, java.time.ZoneOffset.UTC))
                .map(this::mapear)
                .all();
    }

    private EventoConMeta mapear(io.r2dbc.spi.Readable row) {
        return new EventoConMeta(
                row.get("version", Long.class),
                row.get("ocurrido_en", OffsetDateTime.class).toInstant(),
                json.leer(row.get("tipo", String.class), row.get("payload", String.class))
        );
    }

    public static class ConflictoDeConcurrencia extends RuntimeException {
        public ConflictoDeConcurrencia(String m) { super(m); }
    }
}