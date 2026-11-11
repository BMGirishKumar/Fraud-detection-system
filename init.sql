CREATE TABLE IF NOT EXISTS all_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    card_type VARCHAR(20) NOT NULL,
    city VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_reason VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_cases (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) REFERENCES all_transactions(transaction_id),
    user_id VARCHAR(20) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    city VARCHAR(50) NOT NULL,
    fraud_reason VARCHAR(255) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);