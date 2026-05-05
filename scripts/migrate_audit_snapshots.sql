-- audit_snapshots — daily concentration metrics, one row per (network, day)
-- Populated forward from the moment of installation by capture_audit_snapshot.py
-- via the systemd timer. Historical concentration cannot be reconstructed
-- because validator_geo is not versioned, so the chart always says when
-- the series begins.

CREATE TABLE IF NOT EXISTS audit_snapshots (
    network                TEXT        NOT NULL,
    captured_at            TIMESTAMPTZ NOT NULL,
    total_validators       INTEGER,
    active_validators      INTEGER,
    country_count          INTEGER,
    isp_count              INTEGER,
    asn_count              INTEGER,
    country_hhi            INTEGER,
    asn_hhi                INTEGER,
    stake_country_hhi      INTEGER,
    stake_asn_hhi          INTEGER,
    datacenter_pct         NUMERIC(5,2),
    datacenter_count       INTEGER,
    distinct_subnets_total INTEGER,
    PRIMARY KEY (network, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_audit_snapshots_net_at
    ON audit_snapshots (network, captured_at DESC);
