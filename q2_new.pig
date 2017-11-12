-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Project just the columns of feature we need later
feature_data = FOREACH feature
               GENERATE state_name AS feature_state_name, county AS feature_county;

-- Project just the columns of populated_place we need later
populated_place_data = FOREACH populated_place
                       GENERATE county AS populated_place_county, elevation, population;

-- Join two table
feature_with_populated = JOIN feature_data BY feature_county,
                               populated_place_data BY populated_place_county;

-- Group by
group_feature_with_populated = GROUP feature_with_populated
                               BY feature_with_populated.feature_data::feature_state_name;

-- Calculate
result_bag = FOREACH group_feature_with_populated
             GENERATE feature_with_populated.feature_data::feature_state_name AS state_name,
                       SUM(feature_with_populated.populated_place_data::population) AS population;

-- Distinct
result = DISTINCT result_bag;

sorted_result = ORDER result
                BY state_name ASC;

STORE sorted_result INTO 'q2' USING PigStorage(',');
