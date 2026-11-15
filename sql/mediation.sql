-- GAV after reviewed source mappings. host_metric is the UNION ALL of the
-- Windows 7/10 wrappers, not an unreviewed union of similarly named columns.
CREATE OR REPLACE TEMP VIEW host_cpu_view AS
SELECT event_id, entity_id, event_time_utc, numeric_value AS host_cpu_pct
FROM host_metric
WHERE metric_name = 'host_cpu_pct' AND scope = 'host';

-- User query: CPU and traffic for the same host/minute. Each source must be
-- aggregated BEFORE this join. host_window_features is materialized by Spark.
SELECT entity_id, window_start, host_cpu_mean, host_cpu_max,
       n_network, flow_start_sent_bytes, flow_start_received_bytes,
       has_os, has_network, has_cpu
FROM host_window_features
WHERE entity_id = 'windows10_host'
ORDER BY window_start;

-- flow_start_* is a sum of complete flows assigned to their start window.
-- It is NOT the measured bandwidth during that minute.
