CREATE TABLE IF NOT EXISTS eventos (
                                       id           BIGSERIAL PRIMARY KEY,
                                       stream_id    UUID        NOT NULL,
                                       version      BIGINT      NOT NULL,
                                       tipo         VARCHAR(60) NOT NULL,
    payload      TEXT        NOT NULL,
    ocurrido_en  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_stream_version UNIQUE (stream_id, version)
    );

CREATE TABLE IF NOT EXISTS comandos_procesados (
    clave       VARCHAR(80) PRIMARY KEY,
    respuesta   TEXT        NOT NULL,
    creado_en   TIMESTAMPTZ NOT NULL DEFAULT now()
    );




CREATE INDEX IF NOT EXISTS idx_eventos_stream ON eventos (stream_id, version);