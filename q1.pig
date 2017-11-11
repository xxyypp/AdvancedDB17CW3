-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Join table state and populated place first
--join_state_populated = JOIN state BY state::code,
--                           populated_place BY populated_place::state_code;

-- Find difference
--feature_and_join_state_populated = JOIN feature BY feature::county LEFT,
--                                        join_state_populated BY populated_place::county;

--feature_without_join_state_populated = FILTER feature_and_join_state_populated
--                                       BY feature::

-- Project column feature of feature
feature_data = FOREACH feature
               GENERATE county AS feature_county, state_name AS feature_state_name;

-- Project column feature of state
state_data = FOREACH state
             GENERATE name AS state_data_name;

feature_and_state_bag = JOIN feature_data BY feature_county LEFT,
                         state_data BY state_data_name;

feature_and_state = DISTINCT feature_and_state_bag;

feature_without_state = FILTER feature_and_state
                        BY feature_state_name IS NULL;

feature_statename_not_in_state = FOREACH feature_without_state
                                 GENERATE state_data_name;

order_result = ORDER feature_statename_not_in_state
               BY state_data_name ASC;

STORE order_result INTO 'q1' USING PigStorage(',');