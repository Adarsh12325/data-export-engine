#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS records CASCADE;
    
    CREATE TABLE records (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        name VARCHAR(255) NOT NULL,
        value DECIMAL(18, 4) NOT NULL,
        metadata JSONB NOT NULL
    );
    
    -- Generate 10M rows efficiently using generate_series
    INSERT INTO records (name, value, metadata)
    SELECT 
        'Record-' || generate_series,
        (RANDOM() * 10000)::NUMERIC(18,4),
        jsonb_build_object(
            'category', CASE 
                WHEN generate_series % 3 = 0 THEN 'premium'
                WHEN generate_series % 3 = 1 THEN 'standard'
                ELSE 'basic'
            END,
            'tags', array_to_json(ARRAY['tag' || (generate_series % 10)::text]),
            'nested', jsonb_build_object(
                'location', jsonb_build_object(
                    'city', 'City-' || (generate_series % 100),
                    'country', 'Country-' || (generate_series % 50)
                )
            )
        )
    FROM generate_series(1, 10000000);
EOSQL
