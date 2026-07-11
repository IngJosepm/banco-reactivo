package com.ledger.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.domain.EventoCuenta;
import org.springframework.stereotype.Component;

@Component
public class EventoJson {

    private final ObjectMapper mapper;

    public EventoJson(ObjectMapper mapper) { this.mapper = mapper; }

    public String escribir(EventoCuenta e) {
        try { return mapper.writeValueAsString(e); }
        catch (Exception ex) { throw new IllegalStateException("No se pudo serializar", ex); }
    }

    public EventoCuenta leer(String tipo, String payload) {
        try {
            Class<? extends EventoCuenta> clase = switch (tipo) {
                case "CuentaAbierta"     -> EventoCuenta.CuentaAbierta.class;
                case "DineroDepositado"  -> EventoCuenta.DineroDepositado.class;
                case "DineroRetirado"    -> EventoCuenta.DineroRetirado.class;
                case "TransferenciaEnviada"   -> EventoCuenta.TransferenciaEnviada.class;
                case "TransferenciaRecibida"  -> EventoCuenta.TransferenciaRecibida.class;
                default -> throw new IllegalStateException("Evento desconocido: " + tipo);
            };
            return mapper.readValue(payload, clase);
        } catch (Exception ex) {
            throw new IllegalStateException("No se pudo deserializar " + tipo, ex);
        }
    }
}