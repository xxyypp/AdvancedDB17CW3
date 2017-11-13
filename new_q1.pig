-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

--Project column feature of feature
feature_data = FOREACH feature
               GENERATE county, UPPER(state_name) AS feature_statename;

--Project column feature of state
state_data = FOREACH state
             GENERATE name;

-- Join two table
state_and_feature_bag = JOIN feature_data BY feature_statename LEFT,
                          state_data BY name;

--state_and_feature = DISTINCT state_and_feature_bag;

res = ORDER state_and_feature_bag BY feature_statename;

STORE res INTO 'q1_no_distinct' USING PigStorage(',');
