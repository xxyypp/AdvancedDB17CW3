-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

--Project column feature of feature
feature_data = FOREACH feature
               GENERATE county, UPPER(state_name) AS feature_statename;

--Project column feature of state
state_data = FOREACH state
             GENERATE name;

-- Join two table
state_and_feature = JOIN feature_data BY feature_statename LEFT,
                          state_data BY name;

feature_not_in_state = FILTER state_and_feature
                       BY state_data::name IS NULL;

--Generate the result
result = FOREACH feature_not_in_state
         GENERATE feature_data::feature_statename AS state_name;

-- Order the result
sorted_result = ORDER result
               BY state_name ASC;

STORE sorted_result INTO 'new_q1' USING PigStorage(',');