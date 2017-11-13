-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Load populated_place table
populated_data_with_null = FOREACH populated_place
                           GENERATE name, state_code, population;

-- Filter NULL value
populated_data = FILTER populated_data_with_null
                 BY population IS NOT NULL;

-- Load populated_place table
state_data = FOREACH state
             GENERATE code, name AS state_data_name;

-- Group by
group_populated_data = GROUP populated_data BY state_code;

-- Select top 5 populated places
top_five = FOREACH group_populated_data{
                sorted = ORDER populated_data BY population DESC, name DESC;
                top = LIMIT sorted 5;
           GENERATE group AS state_code, FLATTEN(top);
           }

-- Join with state
join_result = JOIN state_data BY code,
              top_five BY state_code;

-- Result
result = FOREACH join_result
         GENERATE state_data_name AS state_name, name AS name ,population AS population;


STORE result INTO 'q4' USING PigStorage(',');