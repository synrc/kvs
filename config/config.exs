use Mix.Config

config :kvs,
  dba: :kvs_rocks,
  dba_st: :kvs_st,
  seq_dba: :kvs_rocks,
  schema: [:kvs, :kvs_stream]
