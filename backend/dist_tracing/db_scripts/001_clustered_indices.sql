CREATE INDEX idx_date_created_traffic ON dist_tracing_traffic (date_created);
CLUSTER idx_date_created_traffic ON dist_tracing_traffic;

CREATE INDEX idx_date_created_span ON dist_tracing_span (date_created);
CLUSTER idx_date_created_span ON dist_tracing_span;

CREATE INDEX idx_start_end_time_trace ON dist_tracing_trace (start_time, end_time);
CLUSTER idx_start_end_time_trace ON dist_tracing_trace;
