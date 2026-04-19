
CREATE TABLE etl_rules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    code_b64 TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE etl_run_logs (
    id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES etl_rules(id),
    status VARCHAR(50) NOT NULL,
    result TEXT,
    error TEXT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

INSERT INTO etl_rules (name, description, code_b64) VALUES
(
  'Пример: Hello World',
  'Базовый тест — выводит строку в результат',
  'aW1wb3J0IGpzb24KcmVzdWx0ID0geydzdGF0dXMnOiAnb2snLCAnbWVzc2FnZSc6ICdIZWxsbyBmcm9tIEVUTCEnfQpvdXRwdXQgPSBqc29uLmR1bXBzKHJlc3VsdCk='
);
