backend = System.get_env("KVS_BACKEND", "mnesia")

excludes =
  case backend do
    "rocksdb" -> [:mnesia]
    "mnesia" -> [:rocksdb]
    _ -> []
  end

ExUnit.start(exclude: excludes)
