-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Load feature table
feature_data = FOREACH feature
               GENERATE state_name, county, type;

feature_detail = GROUP feature_data BY (state_name, county);

state_ppl_stream = FOREACH feature_detail {
                        ppl_type = FILTER feature_data
                                   BY type == 'ppl';
                        stream_type = FILTER feature_data
                                      BY type == 'stream';
                   GENERATE group.state_name AS state_name,
                             group.county AS county,
                             COUNT(ppl_type.type) AS no_ppl,
                             COUNT(stream_type.type) AS no_stream;
                   }

sorted_result = ORDER state_ppl_stream
                 BY state_name, county;

result_no_null_county = FILTER sorted_result
                        BY (county != 'null') ;

STORE result_no_null_county INTO 'q3' USING PigStorage(',');