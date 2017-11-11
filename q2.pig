-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

--Project column feature of feature
feature_data = FOREACH feature
               GENERATE county AS feature_county, state_name AS feature_state_name;

--Project column feature of populated_place
populated_place_data = FOREACH populated_place
                       GENERATE county AS populated_county, elevation AS populated_elevation, population AS populated_population;

--Join feature and populated
join_feature_populated_bag = JOIN feature_data BY feature_county,
                                  populated_place_data BY populated_county;

group_the_result = GROUP join_feature_populated_bag BY feature_state_name;

--Calculate state details
state_details = FOREACH group_the_result
                GENERATE join_feature_populated_bag.feature_data::feature_state_name AS state_name,
                          COUNT(join_feature_populated_bag.populated_place_data::populated_population) AS population,
                          AVG(join_feature_populated_bag.populated_place_data::populated_elevation) AS elevation;

sorted_result = ORDER state_details
                BY state_name ASC;

STORE sorted_result INTO 'q2' USING PigStorage(',');