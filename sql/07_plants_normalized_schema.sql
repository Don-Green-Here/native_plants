USE native_plants;
CREATE TABLE IF NOT EXISTS plant_traits_normalized (
id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

symbol VARCHAR(16) NOT NULL, 
trait_key VARCHAR(64) NOT NULL,
trait_value VARCHAR(255) NOT NULL,
value_type ENUM('enum', 'bool','number','text') NOT NULL DEFAULT 'text', 

source_system VARCHAR(64) NOT NULL DEFAULT 'USDA_SELENIUM_BS4', 

trait_name_raw VARCHAR(255) NULL,
trait_value_raw VARCHAR(255) NULL,

last_computed_at DATETIME(6) NOT NULL, 

UNIQUE KEY uq_symbol_trait (symbol, trait_key),
INDEX idx_trait_key (trait_key),
INDEX idx_trait_key_value (trait_key, trait_value), 

CONSTRAINT fk_traits_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol) 
);