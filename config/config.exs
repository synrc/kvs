import Config

config :kvs,
  dba: :kvs_mnesia,
  dba_st: :kvs_stream,
  schema: [:kvs, :kvs_stream]

import_config "#{config_env()}.exs"
