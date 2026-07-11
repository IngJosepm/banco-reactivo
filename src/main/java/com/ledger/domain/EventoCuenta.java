package com.ledger.domain;

import java.math.BigDecimal;
import java.util.UUID;

public sealed interface EventoCuenta permits EventoCuenta.CuentaAbierta,
        EventoCuenta.DineroDepositado,
        EventoCuenta.DineroRetirado,
        EventoCuenta.TransferenciaEnviada,
        EventoCuenta.TransferenciaRecibida {

    record CuentaAbierta(UUID cuentaId, String titular) implements EventoCuenta {
    }

    record DineroDepositado(UUID cuentaId, BigDecimal monto, String concepto) implements EventoCuenta {
    }

    record DineroRetirado(UUID cuentaId, BigDecimal monto, String concepto) implements EventoCuenta {
    }

    record TransferenciaEnviada(UUID cuentaId, UUID destinoId, BigDecimal monto,
                                String concepto, String transferenciaId) implements EventoCuenta {}

    record TransferenciaRecibida(UUID cuentaId, UUID origenId, BigDecimal monto,
                                 String concepto, String transferenciaId) implements EventoCuenta {}

}






