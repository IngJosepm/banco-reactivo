package com.ledger.domain;

import java.math.BigDecimal;
import java.util.UUID;

public record Cuenta(UUID id, String titular, BigDecimal saldo, long version) {

    public static final Cuenta VACIA = new Cuenta(null, null, BigDecimal.ZERO, 0L);

    public boolean existe() { return id != null; }

    public Cuenta aplicar(EventoConMeta m) {
        return switch (m.evento()) {
            case EventoCuenta.CuentaAbierta e ->
                    new Cuenta(e.cuentaId(), e.titular(), BigDecimal.ZERO, m.version());
            case EventoCuenta.DineroDepositado e ->
                    new Cuenta(id, titular, saldo.add(e.monto()), m.version());
            case EventoCuenta.DineroRetirado e ->
                    new Cuenta(id, titular, saldo.subtract(e.monto()), m.version());
            case EventoCuenta.TransferenciaEnviada e->
                    new Cuenta(id,titular,saldo.subtract(e.monto()),m.version());
            case EventoCuenta.TransferenciaRecibida e->
                    new Cuenta(id,titular,saldo.add(e.monto()),m.version());

        };
    }
}