-- Database Schema for Data Reliability Guardrails

CREATE TYPE run_status AS ENUM ('PASSED', 'FAILED');
CREATE TYPE severity_level AS ENUM ('WARN', 'BLOCK');
CREATE TYPE incident_status AS ENUM ('OPEN', 'RESOLVED');

CREATE TABLE pipeline_runs (
    run_id UUID PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    status run_status,
    notes TEXT
);

CREATE TABLE check_results (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES pipeline_runs(run_id),
    check_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    metric_value TEXT,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE incidents (
    incident_id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES pipeline_runs(run_id),
    severity severity_level NOT NULL,
    status incident_status NOT NULL DEFAULT 'OPEN',
    summary TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE downstream_gate (
    gate_id INT PRIMARY KEY DEFAULT 1,
    blocked BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT single_row CHECK (gate_id = 1)
);

-- Initialize the gate row
INSERT INTO downstream_gate (gate_id, blocked, reason) VALUES (1, FALSE, 'System initialized');
