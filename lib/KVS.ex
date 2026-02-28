defmodule KVS do
  use Application
  require Record

  @moduledoc "KVS Abstract Chain Database"

  Record.defrecord(:schema, name: nil, tables: [])
  Record.defrecord(:table,
    name: nil,
    container: false,
    type: :set,
    fields: [],
    keys: [],
    copy_type: :disc_copies,
    instance: {},
    mappings: []
  )

  Record.defrecord(:writer, id: [], count: 0, args: [], cache: [], first: [])
  Record.defrecord(:reader, id: [], pos: 0, cache: [], args: [], feed: [], seek: [], count: 0, dir: 0)

  Record.defrecord(:id_seq, thing: [], id: 0)
  Record.defrecord(:it, id: [])
  Record.defrecord(:ite, id: [], next: [])
  Record.defrecord(:iter, id: [], next: [], prev: [])
  Record.defrecord(:kvs, mod: :kvs_mnesia, st: :kvs_stream, db: [], cx: [])
end

defmodule :kvs do
  use Application
  require KVS
  import KVS

  def start(_type, _args) do
    Supervisor.start_link([], strategy: :one_for_one, name: :kvs_sup)
  end

  def dba, do: Application.get_env(:kvs, :dba, :kvs_mnesia)
  def seq_dba, do: Application.get_env(:kvs, :dba_seq, :kvs_mnesia)
  def db, do: dba().db()
  def kvs_stream, do: Application.get_env(:kvs, :dba_st, :kvs_stream)

  # kvs api
  def all(feed) when is_binary(feed) or is_list(feed), do: feed(feed)
  def all(table), do: all(table, kvs(mod: dba(), db: db()))
  def delete(table, key), do: delete(table, key, kvs(mod: dba(), db: db()))
  def get(table, key), do: __MODULE__.get(table, key, kvs(mod: dba(), db: db()))
  def index(table, k, v), do: index(table, k, v, kvs(mod: dba()))
  def keys(feed), do: keys(feed, kvs(mod: dba(), db: db()))
  def key_match(feed, id), do: key_match(feed, id, kvs(mod: dba(), db: db()))
  def match(record), do: match(record, kvs(mod: dba()))
  def index_match(record, index), do: index_match(record, index, kvs(mod: dba()))
  def join(), do: join([], kvs(mod: dba(), db: db()))
  def dump(), do: dump(kvs(mod: dba()))
  def join(node), do: join(node, kvs(mod: dba(), db: db()))
  def leave(), do: leave(kvs(mod: dba(), db: db()))
  def destroy(), do: destroy(kvs(mod: dba(), db: db()))
  def count(table), do: count(table, kvs(mod: dba()))
  def put(record), do: __MODULE__.put(record, kvs(mod: dba(), db: db()))
  def stop(), do: stop_kvs(kvs(mod: dba()))
  def start(), do: start(kvs(mod: dba()))
  def ver(), do: ver(kvs(mod: dba()))
  def dir(), do: dir(kvs(mod: dba()))
  def feed(key), do: feed(key, kvs(mod: dba(), st: kvs_stream(), db: db()))
  def seq([], dx), do: seq([], dx, kvs(mod: :kvs_rocks))
  def seq(table, dx), do: seq(table, dx, kvs(mod: seq_dba()))
  def remove(rec, feed), do: remove(rec, feed, kvs(mod: dba(), st: kvs_stream(), db: db()))

  def put(records, kvs(mod: mod, db: db)) when is_list(records), do: mod.put(records, db)
  def put(record, kvs(mod: mod, db: db)), do: mod.put(record, db)

  def get(record_name, key, kvs(mod: mod, db: db)), do: mod.get(record_name, key, db)
  def delete(tab, key, kvs(mod: mod, db: db)), do: mod.delete(tab, key, db)
  def delete_range(feed, last, kvs(mod: dba, db: db)), do: dba.delete_range(feed, last, db)
  def count(tab, kvs(mod: dba)), do: dba.count(tab)
  def index(tab, key, value, kvs(mod: dba)), do: dba.index(tab, key, value)
  def keys(feed, kvs(mod: dba, db: db)), do: dba.keys(feed, db)
  def key_match(feed, id, kvs(mod: dba, db: db)), do: dba.key_match(feed, id, db)
  def match(record, kvs(mod: dba)), do: dba.match(record)
  def index_match(record, index, kvs(mod: dba)), do: dba.index_match(record, index)
  def seq(tab, incr, kvs(mod: dba)), do: dba.seq(tab, incr)
  def dump(kvs(mod: mod)), do: mod.dump()
  def feed(tab, kvs(st: mod, db: db)), do: mod.feed(tab, db)
  def remove(rec, feed, kvs(st: mod, db: db)), do: mod.remove(rec, feed, db)
  def all(tab, kvs(mod: dba, db: db)), do: dba.all(tab, db)
  def start(kvs(mod: dba)), do: dba.start()
  def stop_kvs(kvs(mod: dba)), do: dba.stop()
  def join(node, kvs(mod: dba, db: db)), do: dba.join(node, db)
  def leave(kvs(mod: dba, db: db)), do: dba.leave(db)
  def destroy(kvs(mod: dba, db: db)), do: dba.destroy(db)
  def ver(kvs(mod: dba)), do: dba.version()
  def dir(kvs(mod: dba)), do: dba.dir()

  # stream api
  def top(x), do: kvs_stream().top(x)
  def top(x, kvs(db: db)), do: kvs_stream().top(x, db)
  def bot(x), do: kvs_stream().bot(x)
  def bot(x, kvs(db: db)), do: kvs_stream().bot(x, db)
  def next(x), do: kvs_stream().next(x)
  def next(x, kvs(db: db)), do: kvs_stream().next(x, db)
  def prev(x), do: kvs_stream().prev(x)
  def prev(x, kvs(db: db)), do: kvs_stream().prev(x, db)
  def drop(x), do: kvs_stream().drop(x)
  def drop(x, kvs(db: db)), do: kvs_stream().drop(x, db)
  def take(x), do: kvs_stream().take(x)
  def take(x, kvs(db: db)), do: kvs_stream().take(x, db)
  def save(x), do: kvs_stream().save(x)
  def save(x, kvs(db: db)), do: kvs_stream().save(x, db)
  def remove(x), do: kvs_stream().remove(x)
  def cut(x), do: kvs_stream().cut(x)
  def cut(x, kvs(db: db)), do: kvs_stream().cut(x, db)
  def add(x), do: kvs_stream().add(x)
  def add(x, kvs(db: db)), do: kvs_stream().add(x, db)
  def append(x, y), do: kvs_stream().append(x, y)
  def append(x, y, kvs(db: db)), do: kvs_stream().append(x, y, db)
  def load_reader(x), do: kvs_stream().load_reader(x)
  def load_reader(x, kvs(db: db)), do: kvs_stream().load_reader(x, db)
  def get_writer(x), do: kvs_stream().get_writer(x)
  def get_writer(x, kvs(db: db)), do: kvs_stream().get_writer(x, db)
  def get_reader(x), do: kvs_stream().get_reader(x)
  def get_reader(x, kvs(db: db)), do: kvs_stream().get_reader(x, db)

  def ensure(w = writer()), do: ensure(w, kvs(mod: dba(), db: db()))
  def ensure(writer(id: id), x = kvs()) do
    case get(:writer, id, x) do
      {:error, _} ->
        save(writer(id: id), x)
        :ok
      {:ok, _} ->
        :ok
    end
  end

  def cursors() do
    modules()
    |> Enum.flat_map(fn m ->
      m.metainfo()
      |> schema(:tables)
      |> Enum.filter(fn table(name: name) -> name == :reader or name == :writer end)
      |> Enum.map(fn table(name: n, fields: f) -> {n, f} end)
    end)
  end

  # metainfo api
  def tables() do
    modules() |> Enum.flat_map(fn m -> m.metainfo() |> schema(:tables) end)
  end

  def modules(), do: Application.get_env(:kvs, :schema, [])

  def metainfo() do
    schema(name: :kvs, tables: core() ++ test_tabs())
  end

  def core() do
    [
      table(name: :id_seq, fields: [:thing, :id], keys: [:thing]),
      table(name: :reader, fields: [:id, :pos, :cache, :args, :feed, :seek, :count, :dir]),
      table(name: :writer, fields: [:id, :count, :args, :cache, :first])
    ]
  end

  def test_tabs() do
    [table(name: :msg, fields: [:id, :next, :prev, :user, :msg])]
  end

  def get_table(name) when is_atom(name) do
    case Enum.find(tables(), fn table(name: n) -> n == name end) do
      nil -> false
      t -> t
    end
  end
  def get_table(_), do: false

  def seq_gen() do
    f = fn key ->
      case get(:id_seq, key) do
        {:error, _} -> {key, put(id_seq(thing: key, id: 0))}
        {:ok, _} -> {key, :skip}
      end
    end
    cursors() |> Enum.map(fn {name, _} -> f.(to_string(name)) end)
  end

  def initialize(backend, module) do
    module.metainfo()
    |> schema(:tables)
    |> Enum.map(fn t = table(name: name, fields: fields, copy_type: copy, type: type, keys: keys) ->
      backend.create_table(name, [attributes: fields, "#{copy}": [node()], type: type] ++ if(type == :set, do: [], else: [type: type]))
      Enum.each(keys, fn key -> backend.add_table_index(name, key) end)
      t
    end)
  end

  def fields(table_name) when is_atom(table_name) do
    case get_table(table_name) do
      false -> []
      t -> table(t, :fields)
    end
  end

  def defined(record, field) do
    field in fields(elem(record, 0))
  end

  def field(record, field) do
    fields_list = fields(elem(record, 0))
    index = Enum.find_index(fields_list, &(&1 == field)) + 1
    elem(record, index)
  end

  def setfield(record, field, value) do
    fields_list = fields(elem(record, 0))
    index = Enum.find_index(fields_list, &(&1 == field)) + 1
    put_elem(record, index, value)
  end
end
