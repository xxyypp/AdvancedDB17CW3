-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Project just the columns of feature we need later
state_data = FOREACH state
             GENERATE name;

STORE state_data INTO 'unknown' USING PigStorage(',');