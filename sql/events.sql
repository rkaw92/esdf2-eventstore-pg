BEGIN;

CREATE TABLE eventstore.events (
    -- TODO: include an aggregate_type field to avoid event reinterpretation attacks!
    "type" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "id" TEXT NOT NULL,
    "sequence" TEXT NOT NULL,
    "slot" BIGINT NOT NULL,
    "index" BIGINT NOT NULL,
    "committed_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    PRIMARY KEY("sequence", "index")
) PARTITION BY HASH ("sequence");

CREATE TABLE eventstore.events_0
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE eventstore.events_1
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE eventstore.events_2
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE eventstore.events_3
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE eventstore.events_4
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE eventstore.events_5
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE eventstore.events_6
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE eventstore.events_7
    PARTITION OF eventstore.events
    FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE TABLE eventstore.outbox (
    sequence TEXT NOT NULL,
    slot BIGINT NOT NULL,
    committed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY("sequence", "slot")
);

COMMIT;

-- TODO: benchmark with realistic data - 1 billion rows?
-- INSERT INTO eventstore.events (type, payload, sequence, slot, index, id) SELECT 'Defined', '{"EAN":"1231231231231"}', uuid_generate_v4(), 1, 1, uuid_generate_v4() FROM generate_series(1, 10000000) s(i);
