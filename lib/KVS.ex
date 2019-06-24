defmodule KVS do
  require Record

  Enum.each(Record.extract_all(from_lib: "kvs/include/cursors.hrl"), fn {name, definition} ->
    Record.defrecord(name, definition)
  end)

  Enum.each(Record.extract_all(from_lib: "kvs/include/metainfo.hrl"), fn {name, definition} ->
    Record.defrecord(name, definition)
  end)

  Enum.each(Record.extract_all(from_lib: "kvs/include/kvs.hrl"), fn {name, definition} ->
    Record.defrecord(name, definition)
  end)
end
