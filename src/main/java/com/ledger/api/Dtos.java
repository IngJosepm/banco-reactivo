package com.ledger.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class Dtos {

    public record AbrirCuentaRequest(@NotBlank String titular) {
    }

    public record MovimientoRequest(@NotNull BigDecimal monto, String concepto) {
    }

    public record CuentaResponse(UUID id, String titular, BigDecimal saldo, long version) {
    }

    public record EventoResponse(long version, Instant ocurridoEn, String tipo, BigDecimal monto, String concepto) {
    }


    public record EventoStream(String tipo, UUID cuentaId, String titular,
                               BigDecimal monto, BigDecimal saldoResultante,
                               String concepto, Instant momento) {
        public static EventoStream latido() {
            return new EventoStream("PING", null, null, null, null, null, Instant.now());
        }
    }

    public record AlertaFraude(
            UUID cuentaId,
            String titular,
            int cantidad,
            int ventanaSegundos,
            String motivo,
            Instant momento) {}

    public record TransferenciaRequest(
            @NotNull UUID origenId,
            @NotNull UUID destinoId,
            @NotNull BigDecimal monto,
            String concepto) {
    }

    public record TransferenciaResponse(
            String transferenciaId,
            UUID origenId,
            UUID destinoId,
            BigDecimal monto,
            BigDecimal saldoOrigen,
            boolean repetida) {

        public TransferenciaResponse comoRepetida() {
            return new TransferenciaResponse(transferenciaId, origenId, destinoId,
                    monto, saldoOrigen, true);
        }



    }


}