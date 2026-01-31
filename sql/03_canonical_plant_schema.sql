-- 03_canonical_plant_schema.sql 
--  Canonical plant dimensions and state presence bridge (identify plants belonging to multiple states) 
--  Derived from raw_usda_state_plants 
USE native_plants; 
CREATE TABLE IF NOT EXISTS canonical_plants (
	symbol VARCHAR(16) PRIMARY KEY,
	-- Scientific shorthand symbol for the species

	scientific_name_with_author VARCHAR(255) NOT NULL, 
	family VARCHAR(255) NULL, 
	-- Canonical attributes, best from raw, can be enriched later 

	preferred_common_name VARCHAR(255) NULL,
	-- Multiple common names for plants

	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
	-- Audit our injections and updates 

	INDEX idx_family(family),
	INDEX idx_scientific_name(scientific_name_with_author)
);

-- Create a table for the plant presence of a raw fetch snapshot
CREATE TABLE IF NOT EXISTS plant_state_presence (
	id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
	fetch_id BIGINT UNSIGNED NOT NULL, 
	state_code CHAR(2) NOT NULL, 
	symbol VARCHAR(16) NOT NULL,

	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	-- auditing for later

	UNIQUE KEY uq_fetch_state_symbol (fetch_id, state_code, symbol),
	INDEX idx_symbol(symbol), 
	INDEX idx_state(state_code), 
	-- Ensure every fetch is uniquely searchable 

	-- Creating our FOREIGN KEYS to look at original ingestion table state_fetches
	CONSTRAINT fk_presence_fetch FOREIGN KEY (fetch_id) REFERENCES state_fetches(id), 
	CONSTRAINT fk_presence_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
); 

-- Table identifies if a plant has multiple symbols associated, it will help us reconcile duplicates 

CREATE TABLE IF NOT EXISTS plant_synonyms (
	id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
	fetch_id BIGINT UNSIGNED NOT NULL, 
	state_code CHAR(2) NOT NULL,

	symbol VARCHAR(16) NOT NULL,
	synonym_symbol VARCHAR(16) NOT NULL, 

	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

	UNIQUE KEY uq_fetch_state_symbol_syn (fetch_id, state_code, symbol, synonym_symbol),
	INDEX idx_symbol (symbol),
	INDEX idx_synonym (synonym_symbol),

  CONSTRAINT fk_syn_fetch FOREIGN KEY (fetch_id) REFERENCES state_fetches(id),
  CONSTRAINT fk_syn_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
);

