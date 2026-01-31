USE native_plants;
-- 1 row per plant symbol
CREATE TABLE IF NOT EXISTS plant_filter_index (
symbol VARCHAR(16) NOT NULL, 
-- Basic Identity helps avoid extra joins on result lists, not every plant has a common name. 
preferred_common_name VARCHAR(255) NULL, 
scientific_name_with_author VARCHAR(255) NULL, 
family VARCHAR(128) NULL, 

-- Information from profile page
plant_group VARCHAR(64) NULL, 
growth_habit_primary VARCHAR(255) NULL, 
growth_habits_raw VARCHAR(255) NULL,
native_status_raw VARCHAR(255) NULL,
-- Duration can have multiple values, annuals/perrenial depending on zone 
duration_primary ENUM ('Annual', 'Biennial', 'Perennial', 'Unknown') NOT NULL DEFAULT 'Unknown', 
duration_raw VARCHAR(128) NULL,
height_mature_ft DECIMAL(6,2) NULL, 

-- Light requirements
shade_tolerance ENUM('Tolerant', 'Intolerant', 'Intermediate', 'Unknown') NOT NULL DEFAULT 'Unknown', 
is_shade_tolerant TINYINT(1) NOT NULL DEFAULT 0, 

-- Soil Moisture
moisture_use ENUM('Low', 'Medium', 'High', 'Unknown') NOT NULL DEFAULT 'Unknown', 

-- Visibly interesting blooms? 
flower_conspicuous ENUM('Yes', 'No', 'Unknown') NOT NULL DEFAULT 'Unknown',
is_showy_bloomer TINYINT(1) NOT NULL DEFAULT 0, 

bloom_period ENUM(
'Early Spring', 'Spring', 'Mid Spring', 'Late Spring',
'Early Summer', 'Summer', 'Mid Summer', 'Late Summer',
'Early Fall', 'Fall', 'Mid Fall', 'Late Fall',
'Early Winter', 'Winter', 'Mid Winter', 'Late Winter',
'Unknown') NOT NULL DEFAULT 'Unknown', 

-- For ferns/conifers/other non-flowering species. 
is_non_flowering TINYINT(1) NOT NULL DEFAULT 0, 

-- Fall interest
fall_conspicuous ENUM('Yes', 'No', 'Unknown') NOT NULL DEFAULT 'Unknown', 
has_fall_interest TINYINT(1) NOT NULL DEFAULT 0, 

-- Evergreen
leaf_retention ENUM('Yes', 'No', 'Unknown') NOT NULL DEFAULT 'Unknown', 
is_evergreen TINYINT(1) NOT NULL DEFAULT 0, 


-- Completeness of Plant info
has_profile_kv TINYINT(1) NOT NULL DEFAULT 0, 
has_characteristics_kv TINYINT(1) NOT NULL DEFAULT 0, 
last_indexed_at DATETIME(6) NULL, 

PRIMARY KEY (symbol), 
INDEX idx_growth_habit (growth_habit_primary), 
INDEX idx_duration (duration_primary),
INDEX idx_bloom (bloom_period),
INDEX idx_moisture (moisture_use),
INDEX idx_shade_tol (shade_tolerance),
INDEX idx_height (height_mature_ft),
INDEX idx_showy_bloomer (is_showy_bloomer), 
INDEX idx_fall_interest (has_fall_interest),
INDEX idx_evergreen (is_evergreen),

CONSTRAINT fk_filter_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
); 

-- For multiple durations existing, make a separate table 
CREATE TABLE IF NOT EXISTS plant_durations (
symbol VARCHAR(16) NOT NULL, 
duration ENUM('Annual', 'Biennial', 'Perennial') NOT NULL,
PRIMARY KEY (symbol, duration), 
CONSTRAINT fk_duration_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
);


CREATE TABLE IF NOT EXISTS plant_growth_habits(
symbol VARCHAR(16), 
growth_habit VARCHAR(255) NOT NULL, 
PRIMARY KEY (symbol, growth_habit), 
INDEX idx_habit(growth_habit), 
CONSTRAINT fk_growth_symbol FOREIGN KEY (symbol) REFERENCES canonical_plants(symbol)
);




