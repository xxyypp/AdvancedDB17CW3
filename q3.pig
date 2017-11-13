-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Load feature table
state_data = FOREACH feature
             GENERATE state_name, county, type;

state_detail = GROUP state_data BY (state_name, county);

state_ppl_stream = FOREACH state_detail {
                        ppl_type = FILTER state_data
                                   BY type == 'ppl';
                        stream_type = FILTER state_data
                                      BY type == 'stream';
                   GENERATE group.state_name AS state_name,
                             group.county AS county,
                             COUNT(ppl_type.type) AS no_ppl,
                             COUNT(stream_type.type) AS no_stream;
                   }

sorted_result = ORDER state_ppl_stream
                 BY state_name, county;

STORE sorted_result INTO 'q3' USING PigStorage(',');