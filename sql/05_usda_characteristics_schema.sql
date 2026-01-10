USE native_plants; 

CREATE TABLE IF NOT EXISTS plant_characteristics_fetches(
-- This table will tell us if the plant characteristics exist on the page
-- So we do not attempt a page that has NO DATA
symbol VARCHAR(16),
profile_url VARCHAR(512), 
fetched_at DATETIME(6), 
fetch_status ENUM ('HAS_DATA', 'NO_DATA', 'ERROR') NOT NULL, 
error TEXT NULL, 
PRIMARY KEY (symbol), 
INDEX idx_status (fetch_status), 
CONSTRAINT fk_char_fetch_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
); 

CREATE TABLE IF NOT EXISTS plant_characteristics_kv(
-- Key value pairs for fast searching, this holds the exact values
id BIGINT UNIQUE AUTO_INCREMENT PRIMARY KEY,
symbol VARCHAR(16), 
profile_url VARCHAR(512), 
section VARCHAR(64), 
trait_name VARCHAR(64), 
trait_value TEXT NOT NULL,
-- Use SHA-256 for text if necessary 
fetched_at DATETIME(6),    
UNIQUE KEY uq_symbol_section_name_value (symbol, section, trait_name, trait_value(255)),
INDEX idx_symbol (symbol), 
INDEX idx_section (section), 
INDEX idx_trait_name (trait_name), 
CONSTRAINT fk_char_kv_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)

);