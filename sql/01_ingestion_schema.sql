-- 01_ingestion_schema.sql 

USE native_plants; 
CREATE TABLE IF NOT EXISTS states (
state_code CHAR(2) PRIMARY KEY, 
state_name VARCHAR(64) NOT NULL, 
state_slug VARCHAR(64) NOT NULL,
is_active TINYINT NOT NULL DEFAULT 1,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
); 

--  state_slug will be a "url" friendly name, ex/ north-carolina
--  TINYINT will default to 1, BOOLEAN logic

CREATE TABLE IF NOT EXISTS state_fetches (
id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
state_code CHAR(2) NOT NULL, 
url VARCHAR(512) NOT NULL,
fetched_at DATETIME(6) NOT NULL,
http_status SMALLINT NULL,
content_type VARCHAR(128) NULL,
body LONGTEXT NULL,
error TEXT NULL,
INDEX idx_state_code (state_code),
INDEX idx_fetched_at (fetched_at), 
CONSTRAINT fk_fetch_state FOREIGN KEY (state_code) REFERENCES states(state_code) 
);


