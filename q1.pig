-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

--Project column feature of feature
feature_data = FOREACH feature
               GENERATE county AS feature_county, state_name AS feature_state_name;

--Project column feature of state
state_data = FOREACH state
             GENERATE code AS state_code, name AS state_name;

--Project column feature of populated_place
populated_place_data = FOREACH populated_place
                       GENERATE county AS populated_county, state_code AS populated_state_code;

--Join state & populated data
join_state_populated_bag = JOIN state_data BY state_code,
                            populated_place_data BY populated_state_code;

--Set based
join_state_populated = DISTINCT join_state_populated_bag;

--Find difference
feature_and_join_bag = JOIN feature_data BY feature_county LEFT,
                        join_state_populated BY populated_county;

feature_and_join = DISTINCT feature_and_join_bag;

feature_without_join = FILTER feature_and_join
                       BY join_state_populated::state_data::state_code IS NULL;

--Find state_name
state_name_not_in_state_bag = FOREACH feature_without_join
                              GENERATE UPPER(feature_state_name) AS state_name;

state_name_not_in_state = DISTINCT state_name_not_in_state_bag;

--Order
sorted_result = ORDER state_name_not_in_state
                BY state_name ASC;

STORE sorted_result INTO 'q1' USING PigStorage(',');                 