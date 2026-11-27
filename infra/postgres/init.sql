-- Predictions temps reel (voie chaude)
CREATE TABLE IF NOT EXISTS predictions (
    event_id      TEXT PRIMARY KEY,                  -- identifiant deterministe : ecriture idempotente
    vehicle_id    BIGINT NOT NULL,
    time_step     DOUBLE PRECISION NOT NULL,
    probability   DOUBLE PRECISION NOT NULL,         -- probabilite de reparation dans les 48 pas
    prediction    SMALLINT NOT NULL,                 -- 1 = alerte, 0 = normal
    model_version TEXT NOT NULL,
    produced_at   TIMESTAMPTZ,                       -- emission par le producteur
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now() -- ecriture en base : mesure du delai
);
CREATE INDEX IF NOT EXISTS idx_predictions_vehicle ON predictions (vehicle_id, time_step DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_processed ON predictions (processed_at);

-- Micro-batchs deja ecrits : garantit l'exactly-once lors d'une reprise
CREATE TABLE IF NOT EXISTS committed_batches (
    query_name   TEXT NOT NULL,
    batch_id     BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

-- Dernier etat connu de chaque camion (pour le tableau de bord)
CREATE OR REPLACE VIEW vehicle_latest AS
SELECT DISTINCT ON (vehicle_id) vehicle_id, time_step, probability, prediction, processed_at
FROM predictions
ORDER BY vehicle_id, time_step DESC;