CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS operational;

CREATE TABLE operational.merchants (

    merchant_id VARCHAR(50) PRIMARY KEY, 
    merchant_name VARCHAR(50) NOT NULL,
    country VARCHAR(2), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE operational.users (

    user_id VARCHAR(50) PRIMARY KEY, 
    email VARCHAR(255) UNIQUE NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
); 

CREATE TABLE operational.payment_intents (

    intent_id VARCHAR(50) PRIMARY KEY, 
    transaction_id VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    user_id VARCHAR(50) REFERENCES operational.users(user_id),
    merchant_id VARCHAR(50) REFERENCES operational.merchants(merchant_id), 
    amount_minor BIGINT NOT NULL, 
    currency VARCHAR(3) NOT NULL, 
    status VARCHAR(50) NOT NULL, 
    source_system VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_id, idempotency_key)
); 

CREATE TABLE operational.charges(

    charge_id VARCHAR(50) PRIMARY KEY, 
    transaction_id VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    intent_id VARCHAR(50) REFERENCES operational.payment_intents(intent_id), 
    amount_minor BIGINT NOT NULL, 
    status VARCHAR(50) NOT NULL, 
    currency VARCHAR(3) NOT NULL,
    payment_method VARCHAR(50),
    source_system VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_id, idempotency_key)
);

CREATE TABLE operational.refunds(
    refund_id VARCHAR(50) PRIMARY KEY, 
    transaction_id VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    charge_id VARCHAR(50) REFERENCES operational.charges(charge_id), 
    amount_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL,
    refund_reason VARCHAR(100),
    is_partial BOOLEAN DEFAULT FALSE,
    status VARCHAR(50) NOT NULL,
    source_system VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_id, idempotency_key)
);

CREATE TABLE operational.chargebacks (
    chargeback_id VARCHAR(50) PRIMARY KEY,
    charge_id VARCHAR(50) REFERENCES operational.charges(charge_id),
    amount_minor BIGINT NOT NULL,
    reason VARCHAR(100),
    filed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE operational.payment_status_history (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    intent_id VARCHAR(50),
    charge_id VARCHAR(50),
    refund_id VARCHAR(50),
    event_type VARCHAR(50) NOT NULL, -- 'created', 'updated', 'failed', etc.
    chargeback_id VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    payload JSONB NOT NULL,
    CONSTRAINT one_entity CHECK (
        (CASE WHEN intent_id IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN charge_id IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN refund_id IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN chargeback_id IS NOT NULL THEN 1 ELSE 0 END) = 1
    ),
    FOREIGN KEY (intent_id) REFERENCES operational.payment_intents(intent_id),
    FOREIGN KEY (charge_id) REFERENCES operational.charges(charge_id),
    FOREIGN KEY (refund_id) REFERENCES operational.refunds(refund_id),
    FOREIGN KEY (chargeback_id) REFERENCES operational.chargebacks(chargeback_id),
    UNIQUE(transaction_id, idempotency_key, event_type)
);


CREATE TABLE operational.raw_payment_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    UNIQUE(transaction_id, idempotency_key)
);


CREATE INDEX idx_payment_intents_user_id ON operational.payment_intents(user_id);
CREATE INDEX idx_payment_intents_merchant_id ON operational.payment_intents(merchant_id);
CREATE INDEX idx_charges_intent_id ON operational.charges(intent_id);
CREATE INDEX idx_refunds_charge_id ON operational.refunds(charge_id);
CREATE INDEX idx_chargebacks_charge_id ON operational.chargebacks(charge_id);

CREATE INDEX idx_payment_status_history_intent_id ON operational.payment_status_history(intent_id) WHERE intent_id IS NOT NULL;
CREATE INDEX idx_payment_status_history_charge_id ON operational.payment_status_history(charge_id) WHERE charge_id IS NOT NULL;
CREATE INDEX idx_payment_status_history_refund_id ON operational.payment_status_history(refund_id) WHERE refund_id IS NOT NULL;
CREATE INDEX idx_payment_status_history_event_ts ON operational.payment_status_history(event_timestamp);


CREATE PUBLICATION dbz_publication FOR TABLE operational.payment_status_history;