import Config

backend = System.get_env("KVS_BACKEND") || "mnesia"

if backend == "rocksdb" do
  config :kvs,
    dba: :kvs_rocks,
    dba_st: :kvs_st,
    schema: [:kvs]
else
  config :kvs,
    dba: :kvs_mnesia,
    dba_st: :kvs_stream,
    schema: [:kvs, :kvs_stream]
end

import_config "#{config_env()}.exs"
