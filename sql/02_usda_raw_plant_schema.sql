-- 02_usda_raw_plant_schema.sql 
# Raw, parsed USDA state plant records 
# This table stores structured rows parsed from the USDA *_NRCS_csv.txt files
# Lifestage cycle: RAW (parsed, not canonicalized) 
# Source: USDA PLANTS database 

USE native_plants; 
CREATE TABLE IF NOT EXISTS raw_usda_state_plants (
id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 

#Provenance 
state_code CHAR(2) NOT NULL, 
fetch_id BIGINT UNSIGNED NOT NULL,  

#USDA CSV fields 
symbol VARCHAR(16) NOT NULL,
synonym_symbol VARCHAR(16) NULL,
scientific_name_with_author VARCHAR(255) NOT NULL, 
state_common_name VARCHAR(255) NULL, 
family VARCHAR(255) NULL,

#Metadata 
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

#Indices  
INDEX idx_state_symbol (state_code, symbol),
INDEX idx_fetch_id (fetch_id),
#indexing fetch_id useful for joins back to state_fetches, auditing, or deleting/reprocessing a fetch 

#Foreign Keys
CONSTRAINT fk_raw_usda_plants_fetch 
	FOREIGN KEY (fetch_id) 
    REFERENCES state_fetches(id)
);

#Prevent duplicate inserts if the same fetch is parsed twice or more
ALTER TABLE raw_usda_state_plants
  ADD COLUMN synonym_symbol_norm VARCHAR(16)
  GENERATED ALWAYS AS (IFNULL(synonym_symbol, '')) STORED;
		#Will change NULLs to an empty string so that NULL != NULL won't break unqiueness
        #allows safe re-runs with the parser without duplications
        
CREATE UNIQUE INDEX uq_raw_usda_fetch_symbol
	ON raw_usda_state_plants (
		fetch_id,
        symbol, 
        synonym_symbol_norm
        #uses fetch_id + symbol + synonym_symbol_norm as a uniqueness identifier

	);

