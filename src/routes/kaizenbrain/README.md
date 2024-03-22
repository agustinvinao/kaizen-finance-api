Bucket_width = 1 hour

SELECT add_continuous_aggregate_policy(
  continuous_aggregate => 'one_hour_symmary',
  start_offset         => '12 hour',
  end_offset           => '1 minute',
  schedule_interval    => '1 hour')