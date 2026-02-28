defmodule KVS do
  require Record
  @moduledoc "KVS Abstract Chain Database"
  Record.defrecord(:schema, name: nil, tables: [])
  Record.defrecord(:table, name: nil, container: false, type: :set, fields: [], keys: [], copy_type: :disc_copies, instance: {}, mappings: [])
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

  def start(_type, _args) do
    Supervisor.start_link([], strategy: :one_for_one, name: :kvs_sup)
  end

  def dba, do: Application.get_env(:kvs, :dba, :kvs_mnesia)
  def seq_dba, do: Application.get_env(:kvs, :dba_seq, :kvs_mnesia)
  def db, do: dba().db()
  def kvs_stream, do: Application.get_env(:kvs, :dba_st, :kvs_stream)

  # kvs api
  def all(feed) when is_binary(feed) or is_list(feed), do: feed(feed)
  def all(table), do: all(table, KVS.kvs(mod: dba(), db: db()))
  def delete(table, key), do: delete(table, key, KVS.kvs(mod: dba(), db: db()))
  def get(table, key), do: __MODULE__.get(table, key, KVS.kvs(mod: dba(), db: db()))
  def index(table, k, v), do: index(table, k, v, KVS.kvs(mod: dba()))
  def keys(feed), do: keys(feed, KVS.kvs(mod: dba(), db: db()))
  def key_match(feed, id), do: key_match(feed, id, KVS.kvs(mod: dba(), db: db()))
  def match(record), do: match(record, KVS.kvs(mod: dba()))
  def index_match(record, index), do: index_match(record, index, KVS.kvs(mod: dba()))
  def join(), do: join([], KVS.kvs(mod: dba(), db: db()))
  def dump(), do: dump(KVS.kvs(mod: dba()))
  def join(node), do: join(node, KVS.kvs(mod: dba(), db: db()))
  def leave(), do: leave(KVS.kvs(mod: dba(), db: db()))
  def destroy(), do: destroy(KVS.kvs(mod: dba(), db: db()))
  def count(table), do: count(table, KVS.kvs(mod: dba()))
  def put(record), do: __MODULE__.put(record, KVS.kvs(mod: dba(), db: db()))
  def stop(), do: stop(KVS.kvs(mod: dba()))
  def start(), do: start(KVS.kvs(mod: dba()))
  def ver(), do: ver(KVS.kvs(mod: dba()))
  def dir(), do: dir(KVS.kvs(mod: dba()))
  def feed(key), do: feed(key, KVS.kvs(mod: dba(), st: kvs_stream(), db: db()))
  def seq([], dx), do: seq([], dx, KVS.kvs(mod: :kvs_rocks))
  def seq(table, dx), do: seq(table, dx, KVS.kvs(mod: seq_dba()))
  def remove(rec, feed), do: remove(rec, feed, KVS.kvs(mod: dba(), st: kvs_stream(), db: db()))

  def put(records, KVS.kvs(mod: mod, db: db)) when is_list(records), do: mod.put(records, db)
  def put(record, KVS.kvs(mod: mod, db: db)), do: mod.put(record, db)

  def get(record_name, key, KVS.kvs(mod: mod, db: db)), do: mod.get(record_name, key, db)
  def delete(tab, key, KVS.kvs(mod: mod, db: db)), do: mod.delete(tab, key, db)
  def delete_range(feed, last, KVS.kvs(mod: dba, db: db)), do: dba.delete_range(feed, last, db)
  def count(tab, KVS.kvs(mod: dba)), do: dba.count(tab)
  def index(tab, key, value, KVS.kvs(mod: dba)), do: dba.index(tab, key, value)
  def keys(feed, KVS.kvs(mod: dba, db: db)), do: dba.keys(feed, db)
  def key_match(feed, id, KVS.kvs(mod: dba, db: db)), do: dba.key_match(feed, id, db)
  def match(record, KVS.kvs(mod: dba)), do: dba.match(record)
  def index_match(record, index, KVS.kvs(mod: dba)), do: dba.index_match(record, index)
  def seq(tab, incr, KVS.kvs(mod: dba)), do: dba.seq(tab, incr)
  def dump(KVS.kvs(mod: mod)), do: mod.dump()
  def feed(tab, KVS.kvs(st: mod, db: db)), do: mod.feed(tab, db)
  def remove(rec, feed, KVS.kvs(st: mod, db: db)), do: mod.remove(rec, feed, db)
  def all(tab, KVS.kvs(mod: dba, db: db)), do: dba.all(tab, db)
  def start(KVS.kvs(mod: dba)), do: dba.start()
  def stop(KVS.kvs(mod: dba)), do: dba.stop()
  def join(node, KVS.kvs(mod: dba, db: db)), do: dba.join(node, db)
  def leave(KVS.kvs(mod: dba, db: db)), do: dba.leave(db)
  def destroy(KVS.kvs(mod: dba, db: db)), do: dba.destroy(db)
  def ver(KVS.kvs(mod: dba)), do: dba.version()
  def dir(KVS.kvs(mod: dba)), do: dba.dir()

  # stream api
  def top(x), do: kvs_stream().top(x)
  def top(x, KVS.kvs(db: db)), do: kvs_stream().top(x, db)
  def bot(x), do: kvs_stream().bot(x)
  def bot(x, KVS.kvs(db: db)), do: kvs_stream().bot(x, db)
  def next(x), do: kvs_stream().next(x)
  def next(x, KVS.kvs(db: db)), do: kvs_stream().next(x, db)
  def prev(x), do: kvs_stream().prev(x)
  def prev(x, KVS.kvs(db: db)), do: kvs_stream().prev(x, db)
  def drop(x), do: kvs_stream().drop(x)
  def drop(x, KVS.kvs(db: db)), do: kvs_stream().drop(x, db)
  def take(x), do: kvs_stream().take(x)
  def take(x, KVS.kvs(db: db)), do: kvs_stream().take(x, db)
  def save(x), do: kvs_stream().save(x)
  def save(x, KVS.kvs(db: db)), do: kvs_stream().save(x, db)
  def remove(x), do: kvs_stream().remove(x)
  def cut(x), do: kvs_stream().cut(x)
  def cut(x, KVS.kvs(db: db)), do: kvs_stream().cut(x, db)
  def add(x), do: kvs_stream().add(x)
  def add(x, KVS.kvs(db: db)), do: kvs_stream().add(x, db)
  def append(x, y), do: kvs_stream().append(x, y)
  def append(x, y, KVS.kvs(db: db)), do: kvs_stream().append(x, y, db)
  def load_reader(x), do: kvs_stream().load_reader(x)
  def load_reader(x, KVS.kvs(db: db)), do: kvs_stream().load_reader(x, db)
  def writer(x), do: kvs_stream().get_writer(x)
  def writer(x, KVS.kvs(db: db)), do: kvs_stream().get_writer(x, db)
  def reader(x), do: kvs_stream().get_reader(x)
  def reader(x, KVS.kvs(db: db)), do: kvs_stream().get_reader(x, db)

  def ensure(w = KVS.writer()), do: ensure(w, KVS.kvs(mod: dba(), db: db()))
  def ensure(KVS.writer(id: id), x = KVS.kvs()) do
    case get(:writer, id, x) do
      {:error, _} ->
        save(KVS.writer(id: id), x)
        :ok
      {:ok, _} ->
        :ok
    end
  end

  def cursors() do
    modules()
    |> Enum.flat_map(fn m ->
      m.metainfo()
      |> KVS.schema(:tables)
      |> Enum.filter(fn KVS.table(name: name) -> name == :reader or name == :writer end)
      |> Enum.map(fn KVS.table(name: n, fields: f) -> {n, f} end)
    end)
  end

  # metainfo api
  def tables() do
    modules() |> Enum.flat_map(fn m -> m.metainfo() |> KVS.schema(:tables) end)
  end

  def modules(), do: Application.get_env(:kvs, :schema, [])

  def metainfo() do
    KVS.schema(name: :kvs, tables: core() ++ test_tabs())
  end

  def core() do
    [
      KVS.table(name: :id_seq, fields: [:thing, :id], keys: [:thing]),
      KVS.table(name: :reader, fields: [:id, :pos, :cache, :args, :feed, :seek, :count, :dir]),
      KVS.table(name: :writer, fields: [:id, :count, :args, :cache, :first])
    ]
  end

  def test_tabs() do
    [KVS.table(name: :msg, fields: [:id, :next, :prev, :user, :msg])]
  end

  def get_table(name) when is_atom(name) do
    case Enum.find(tables(), fn KVS.table(name: n) -> n == name end) do
      nil -> false
      t -> t
    end
  end
  def get_table(_), do: false

  def seq_gen() do
    f = fn key ->
      case get(:id_seq, key) do
        {:error, _} -> {key, put(KVS.id_seq(thing: key, id: 0))}
        {:ok, _} -> {key, :skip}
      end
    end
    cursors() |> Enum.map(fn {name, _} -> f.(to_string(name)) end)
  end

  def initialize(backend, module) do
    module.metainfo()
    |> KVS.schema(:tables)
    |> Enum.map(fn t = KVS.table(name: name, fields: fields, copy_type: copy, type: type, keys: keys) ->
      backend.create_table(name, [attributes: fields, "#{copy}": [node()], type: type] ++ if(type == :set, do: [], else: [type: type]))
      Enum.each(keys, fn key -> backend.add_table_index(name, key) end)
      t
    end)
  end

  def fields(table_name) when is_atom(table_name) do
    case get_table(table_name) do
      false -> []
      t -> KVS.table(t, :fields)
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
