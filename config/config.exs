import Config

config :kvs,
  dba: :kvs_rocks,
  dba_st: :kvs_st,
  dba_seq: :kvs_rocks,
  seq_pad: [],
  schema: [:kvs, :kvs_stream]
