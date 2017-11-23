-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Project just the columns of feature we need later
state_data = FOREACH state
             GENERATE code, name;

-- Project just the columns of populated_place we need later
populated_place_data = FOREACH populated_place
                       GENERATE state_code, elevation, population;

-- Group the data
group_populated_place_data = GROUP populated_place_data BY state_code;

-- Calculation
population_and_elevation = FOREACH group_populated_place_data
                           GENERATE group AS state_code,
                                     SUM(populated_place_data.population) AS population,
                                     AVG(populated_place_data.elevation) AS elevation;

-- Combine calculation with state
join_result_with_state = JOIN population_and_elevation BY state_code,
                              state_data BY code;

result = FOREACH join_result_with_state
         GENERATE name AS state_name,population AS population,ROUND(elevation) AS elevation;

no_unknown_result = FILTER result
                    BY (state_name != 'UNKNOWN');

-- Order
sorted_result = ORDER no_unknown_result
                BY state_name ASC;

STORE sorted_result INTO 'q2' USING PigStorage(',');