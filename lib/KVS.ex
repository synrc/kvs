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

  defmacro __using__(opts \\ []) do
    imports =
      opts
      |> Macro.expand(__CALLER__)
      |> Keyword.get(:with, [:kvs])

    Enum.map(imports, fn mod ->
      if Code.ensure_compiled(mod) do
        upcased = Module.concat([String.upcase(to_string(mod))])

        quote do
          import unquote(upcased)
          alias unquote(mod), as: unquote(upcased)
        end
      else
        IO.warn("🚨 Unknown module #{mod} was requested to be used by :kvs. Skipping.")
      end
    end)
  end
end
