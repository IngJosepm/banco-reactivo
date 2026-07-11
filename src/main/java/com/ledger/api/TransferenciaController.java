package com.ledger.api;

import com.ledger.api.Dtos.TransferenciaRequest;
import com.ledger.api.Dtos.TransferenciaResponse;
import com.ledger.service.TransferenciaService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/transferencias")
public class TransferenciaController {

    private final TransferenciaService service;

    public TransferenciaController(TransferenciaService service) { this.service = service; }

    @PostMapping
    public Mono<TransferenciaResponse> transferir(
            @RequestHeader("Idempotency-Key") String clave,
            @Valid @RequestBody TransferenciaRequest req) {

        return service.transferir(clave, req.origenId(), req.destinoId(),
                req.monto(), req.concepto());
    }
}