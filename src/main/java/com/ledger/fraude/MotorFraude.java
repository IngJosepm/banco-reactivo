package com.ledger.fraude;

import com.ledger.api.Dtos.AlertaFraude;
import com.ledger.api.Dtos.EventoStream;
import com.ledger.stream.EventoBus;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class MotorFraude {

    private static final Logger log = LoggerFactory.getLogger(MotorFraude.class);

    private static final int UMBRAL = 3;                      // nº de transferencias
    private static final Duration VENTANA = Duration.ofSeconds(10);

    private final EventoBus bus;

    public MotorFraude(EventoBus bus) { this.bus = bus; }

    @PostConstruct
    public void arrancar() {
        bus.flujo()
                .filter(e -> "TransferenciaEnviada".equals(e.tipo()))
                .groupBy(EventoStream::cuentaId)                 // un sub-flujo por cuenta
                .flatMap(porCuenta -> porCuenta
                        .window(VENTANA)                        // ventanas de tiempo por cuenta
                        .flatMap(ventana -> ventana.count()     // cuántas cayeron en la ventana
                                .filter(n -> n >= UMBRAL)
                                .map(n -> crearAlerta(porCuenta.key(), n))))
                .doOnNext(bus::publicarAlerta)
                .retry()                                        // si el motor cae, se reinicia
                .subscribe();

        log.info("Motor antifraude activo: {} transferencias / {}s", UMBRAL, VENTANA.getSeconds());
    }

    private AlertaFraude crearAlerta(UUID cuentaId, long cantidad) {
        log.warn("🚨 ALERTA: cuenta {} -> {} transferencias en {}s",
                cuentaId, cantidad, VENTANA.getSeconds());
        return new AlertaFraude(
                cuentaId,
                null,                                           // el titular lo resolvemos luego si hace falta
                (int) cantidad,
                (int) VENTANA.getSeconds(),
                "Velocidad sospechosa: " + cantidad + " transferencias en " + VENTANA.getSeconds() + "s",
                Instant.now());
    }
}