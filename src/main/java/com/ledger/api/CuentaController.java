package com.ledger.api;

import com.ledger.api.Dtos.*;
import com.ledger.domain.Cuenta;
import com.ledger.domain.EventoConMeta;
import com.ledger.domain.EventoCuenta;
import com.ledger.service.CuentaService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/cuentas")
public class CuentaController {

    private final CuentaService service;

    public CuentaController(CuentaService service) { this.service = service; }

    @PostMapping
    public Mono<CuentaResponse> abrir(@Valid @RequestBody AbrirCuentaRequest req) {
        return service.abrir(req.titular()).map(this::aResponse);
    }

    @PostMapping("/{id}/depositos")
    public Mono<CuentaResponse> depositar(@PathVariable UUID id, @Valid @RequestBody MovimientoRequest req) {
        return service.depositar(id, req.monto(), req.concepto()).map(this::aResponse);
    }

    @PostMapping("/{id}/retiros")
    public Mono<CuentaResponse> retirar(@PathVariable UUID id, @Valid @RequestBody MovimientoRequest req) {
        return service.retirar(id, req.monto(), req.concepto()).map(this::aResponse);
    }

    @GetMapping("/{id}")
    public Mono<CuentaResponse> ver(@PathVariable UUID id) {
        return service.cargar(id).map(this::aResponse);
    }

    /** El log inmutable. Esto ES el extracto bancario. */
    @GetMapping("/{id}/eventos")
    public Flux<EventoResponse> historial(@PathVariable UUID id) {
        return service.historial(id).map(this::aEventoResponse);
    }

    /** Viaje en el tiempo: GET /api/cuentas/{id}/saldo?en=2026-03-03T14:02:00Z */
    @GetMapping("/{id}/saldo")
    public Mono<CuentaResponse> saldoEn(@PathVariable UUID id,
                                        @RequestParam(required = false) Instant en) {
        return (en == null ? service.cargar(id) : service.cargarEn(id, en)).map(this::aResponse);
    }

    public record EventoResponse(long version, Instant ocurridoEn, String tipo,
                                 BigDecimal monto, String concepto, UUID contraparte) {}

    private CuentaResponse aResponse(Cuenta c) {
        return new CuentaResponse(c.id(), c.titular(), c.saldo(), c.version());
    }



    private EventoResponse aEventoResponse(EventoConMeta m) {
        return switch (m.evento()) {
            case EventoCuenta.CuentaAbierta e ->
                    new EventoResponse(m.version(), m.ocurridoEn(), "CuentaAbierta", null, e.titular(), null);
            case EventoCuenta.DineroDepositado e ->
                    new EventoResponse(m.version(), m.ocurridoEn(), "DineroDepositado", e.monto(), e.concepto(), null);
            case EventoCuenta.DineroRetirado e ->
                    new EventoResponse(m.version(), m.ocurridoEn(), "DineroRetirado", e.monto(), e.concepto(), null);
            case EventoCuenta.TransferenciaEnviada e ->
                    new EventoResponse(m.version(), m.ocurridoEn(), "TransferenciaEnviada", e.monto(), e.concepto(), e.destinoId());
            case EventoCuenta.TransferenciaRecibida e ->
                    new EventoResponse(m.version(), m.ocurridoEn(), "TransferenciaRecibida", e.monto(), e.concepto(), e.origenId());
        };
    }



    @GetMapping
    public Flux<CuentaResponse> listar() {
        return service.listar().map(this::aResponse);
    }
}