-- 04_plant_metadata_schema.sql
-- This file will store metadata such as common names, bloom period, soil type, light requirements etc. 
-- Designed for expansion as we add sources beyond the USDA (ex/ state guides, indigineous knowledge) 
-- Separating this from canonical plant data keeps that table stable
USE native_plants;
CREATE TABLE IF NOT EXISTS plant_common_names(
	id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	symbol VARCHAR(16) NOT NULL, 
	common_name VARCHAR(255) NOT NULL, 
    
    state_code CHAR(2) NULL, 
    -- NULL = Not state specific 
    source_system VARCHAR(255) NOT NULL DEFAULT 'USDA_STATE_FILE', 
    is_preferred TINYINT NOT NULL DEFAULT 0,
    
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
	-- Audit our injections and updates 
    
    INDEX idx_symbol(symbol), 
    INDEX idx_common_name(common_name), 
    
    CONSTRAINT fk_common_names_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol) 
	
);
-- Prevent duplicate inserts if the same fetch is parsed twice or more, work around to coalesce not working in this version of sql 
ALTER TABLE plant_common_names
ADD COLUMN state_code_norm VARCHAR(16)
GENERATED ALWAYS AS (IFNULL(state_code, '')) STORED;
-- Will change NULLs to an empty string so that NULL != NULL won't break unqiueness
-- allows safe re-runs with the parser without duplications in our unique key 

CREATE UNIQUE INDEX uq_symbol_name_state_source 
	ON plant_common_names (
        symbol, 
        common_name, 
        state_code_norm,
        source_system
	);
	-- Dedupe 

CREATE TABLE IF NOT EXISTS plant_images(
id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
symbol VARCHAR(16), 

image_url TEXT NOT NULL,
image_url_sha256 BINARY(32) NOT NULL,
-- Need to store url as text because it exceeds our utf8mb4 limits 
-- Negligible Hash collisions with sha-256 in this case, uniqueness constraint, avoids key-length limits

source_system VARCHAR(64) NOT NULL,
license VARCHAR(255) NULL, 
attribution TEXT NULL, 
-- Need to site our image sources 

is_primary TINYINT NOT NULL DEFAULT 0, 
-- primary image we'll use for the plant 

created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
UNIQUE KEY uq_symbol_image (symbol, image_url_sha256),
INDEX idx_symbol(symbol), 

CONSTRAINT fk_images_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
);