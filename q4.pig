-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Load populated_place table
populated_data = FOREACH populated_place
                 GENERATE name, county, state_code, population;

-- Load populated_place table
state_data = FOREACH state
             GENERATE code, name AS state_data_name;

-- Group by
populated_detail = GROUP populated_data BY state_code;

-- Select top 5 populated places
result = FOREACH populated_detail{
              ordered = ORDER populated_data BY population DESC;
              top_five = LIMIT ordered 5;
         GENERATE FLATTEN(group) AS state_code, FLATTEN(top_five.name), FLATTEN(top_five.population);
         }

sorted_result = ORDER result BY state_code;

-- Join with state
--result_with_state = JOIN state_data BY code,
  --                        result BY state_code;

--final_result = FOREACH result_with_state
  --             GENERATE result_with_state.state_data::name AS state_data_name,
    --               result_with_state.result::name AS name,
      --             result_with_state.result::population AS popualtion;

--sorted_result = ORDER final_result
  --               BY state_name;

STORE sorted_result INTO 'q4' USING PigStorage(',');