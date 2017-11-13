-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Project just the columns of feature we need later
feature_data = FOREACH feature
               GENERATE state_name AS feature_state_name, county AS feature_county;

-- Project just the columns of populated_place we need later
populated_place_data = FOREACH populated_place
                       GENERATE county AS populated_place_county, elevation, population;

-- Join pop & feature
join_populated_and_feature = JOIN feature_data BY feature_county,
                                   populated_place_data BY populated_place_county;

-- Optimise
optimise_join_populated_and_feature = FOREACH join_populated_and_feature
                                      GENERATE feature_data::feature_state_name,
                                                populated_place_data::population,
                                                populated_place_data::elevation;
-- Group
group_join_populated_and_feature = GROUP optimise_join_populated_and_feature
                                   BY join_populated_and_feature.feature_data::feature_state_name;

-- Calculation
result = FOREACH group_join_populated_and_feature
         GENERATE group AS state_name,
                   SUM(optimise_join_populated_and_feature.population) AS population,
                   AVG(optimise_join_populated_and_feature.elevation) AS elevation;

-- Order
sorted_result = ORDER result
                BY state_name ASC;

STORE sorted_result INTO 'q2' USING PigStorage(',');