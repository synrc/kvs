ExUnit.start(exclude: if(System.get_env("KVS_BACKEND") == "rocksdb", do: [], else: [:rocksdb]))
