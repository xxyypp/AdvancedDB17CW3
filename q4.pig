-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Load populated_place table
populated_data = FOREACH populated_place
                 GENERATE name, county, state_code, population;

-- Load populated_place table
state_data = FOREACH state
             GENERATE code, name AS state_data_name;

-- Group by
group_populated_data = GROUP populated_data BY state_code;

-- Select top 5 populated places
result = FOREACH group_populated_data{
            order_populated_data = ORDER populated_data
                                   BY population DESC;
            top_five = LIMIT order_populated_data 5;
         GENERATE top_five;
         --GENERATE FLATTEN(top_five.state_code),FLATTEN(top_five.name), FLATTEN(top_five.population) ;
         }


STORE result INTO 'q4' USING PigStorage(',');