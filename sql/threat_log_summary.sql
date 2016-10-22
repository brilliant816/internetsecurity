create table threat_log_summary (
  threat_date DATE,
  threat_type VARCHAR(256),
  url VARCHAR(1024),
  threat_level int,
  threat_count int
);