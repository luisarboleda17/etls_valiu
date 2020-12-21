
CREATE TABLE valiu_core.public.transaction(
    id VARCHAR(36) PRIMARY KEY,
    id2 VARCHAR(24) NOT NULL,
    account_id_dst VARCHAR(36),
    account_id_src VARCHAR(36),
    amount_dst NUMERIC,
    amount_dst_usd NUMERIC,
    amount_src NUMERIC,
    amount_src_usd NUMERIC,
    asset_dst VARCHAR(4),
    asset_src VARCHAR(4),
    contact_dst VARCHAR(100),
    created_at TIMESTAMP,
    description VARCHAR(200),
    order_id VARCHAR(36),
    service_name VARCHAR(100),
    short_id VARCHAR(15),
    state VARCHAR(10),
    sync_date TIMESTAMP,
    type VARCHAR(12),
    updated_at TIMESTAMP,
    user_id VARCHAR(36),
    user_type VARCHAR(6),
    v VARCHAR(3),
    wallet_dst VARCHAR(36),
    wallet_src VARCHAR(36)
);