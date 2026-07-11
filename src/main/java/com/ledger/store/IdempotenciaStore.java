package com.ledger.store;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public class IdempotenciaStore {

    private final DatabaseClient db;

    public IdempotenciaStore(DatabaseClient db) { this.db = db; }

    /** Devuelve la respuesta guardada, o vacío si la clave nunca se usó. */
    public Mono<String> buscar(String clave) {
        return db.sql("SELECT respuesta FROM comandos_procesados WHERE clave = :clave")
                .bind("clave", clave)
                .map(row -> row.get("respuesta", String.class))
                .one();
    }

    /** Registra la clave. Si ya existía, revienta con violación de PK: eso es lo que queremos. */
    public Mono<Long> guardar(String clave, String respuesta) {
        return db.sql("INSERT INTO comandos_procesados (clave, respuesta) VALUES (:clave, :respuesta)")
                .bind("clave", clave)
                .bind("respuesta", respuesta)
                .fetch()
                .rowsUpdated();
    }
}