defmodule :kvs_mnesia do
  require Record
  require KVS

  def db, do: ""
  def seq_pad, do: Application.get_env(:kvs, :seq_pad, [])

  def start, do: :mnesia.start()
  def stop, do: :mnesia.stop()
  def destroy() do
    for {_, t} <- :kvs.dir(), do: :mnesia.delete_table(t)
    :mnesia.stop()
    :mnesia.delete_schema([node()])
    :ok
  end
  def destroy(_), do: destroy()

  def leave, do: :ok
  def leave(_), do: :ok
  def version, do: {:version, "KVS MNESIA"}

  def dir do
    for t <- :mnesia.system_info(:local_tables) do
      {:table, t}
    end
  end

  def join([], _) do
    :mnesia.start()
    :mnesia.change_table_copy_type(:schema, node(), :disc_copies)
    initialize()
  end

  def join(node, _) do
    :mnesia.start()
    :mnesia.change_config(:extra_db_nodes, [node])
    :mnesia.change_table_copy_type(:schema, node(), :disc_copies)

    for {tb, [{n, type}]} <- Enum.map(:mnesia.system_info(:tables), fn t -> {t, :mnesia.table_info(t, :where_to_commit)} end),
        node == n do
      {tb, :mnesia.add_table_copy(tb, node(), type)}
    end
  end

  def initialize() do
    :mnesia.create_schema([node()])
    res = for m <- :kvs.modules(), do: :kvs.initialize(:kvs_mnesia, m)

    tables = for t <- :kvs.tables(), do: KVS.table(t, :name)
    :mnesia.wait_for_tables(tables, :infinity)
    res
  end

  def index(tab, key, value) do
    List.flatten(many(fn -> :mnesia.index_read(tab, value, key) end))
  end

  def keys(tab, _), do: :mnesia.all_keys(tab)
  def key_match(_tab, _id, _), do: []

  def get(record_name, key, _) do
    just_one(fn -> :mnesia.read(record_name, key) end)
  end

  def put(r), do: put(r, db())
  def put(records, _) when is_list(records) do
    void(fn -> Enum.each(records, &:mnesia.write/1) end)
  end
  def put(record, x), do: put([record], x)

  def delete(tab, key, _) do
    case :mnesia.activity(context(), fn -> :mnesia.delete({tab, key}) end) do
      {:aborted, reason} -> {:error, reason}
      {:atomic, _result} -> :ok
      _ -> :ok
    end
  end

  def delete_range(_, _, _), do: {:error, :not_found}
  def match(record), do: List.flatten(many(fn -> :mnesia.match_object(record) end))
  def index_match(record, index), do: List.flatten(many(fn -> :mnesia.index_match_object(record, index) end))

  def count(record_name), do: :mnesia.table_info(record_name, :size)

  def all(r, _) do
    List.flatten(many(fn ->
      for g <- :mnesia.all_keys(r), do: :mnesia.read({r, g})
    end))
  end

  def seq(record_name, []), do: seq(record_name, 1)
  def seq(record_name, incr) do
    val = Integer.to_string(:mnesia.dirty_update_counter({:id_seq, record_name}, incr))
    pad = 20 - byte_size(val)
    if pad > 0 and record_name in seq_pad() do
      String.duplicate("0", pad) <> val
    else
      val
    end
  end

  def many(fun) do
    case :mnesia.activity(context(), fun) do
      {:atomic, [r]} -> r
      {:aborted, error} -> {:error, error}
      x -> x
    end
  end

  def void(fun) do
    case :mnesia.activity(context(), fun) do
      {:atomic, :ok} -> :ok
      {:aborted, error} -> {:error, error}
      x -> x
    end
  end

  def info(t) do
    try do
      :mnesia.table_info(t, :all)
    catch
      _, _ -> []
    end
  end

  def create_table(name, options) do
    case :mnesia.create_table(name, options) do
      {:atomic, :ok} -> :ok
      {:aborted, {:already_exists, _}} -> :ok
      other -> other
    end
  end

  def add_table_index(record, field), do: :mnesia.add_table_index(record, field)

  def exec(q) do
    f = fn -> :qlc.e(q) end
    {:atomic, val} = :mnesia.activity(context(), f)
    val
  end

  def just_one(fun) do
    case :mnesia.activity(context(), fun) do
      {:atomic, []} -> {:error, :not_found}
      {:atomic, [r]} -> {:ok, r}
      [] -> {:error, :not_found}
      [r] -> {:ok, r}
      r when is_list(r) -> {:ok, r}
      error -> error
    end
  end

  def context, do: Application.get_env(:kvs, :mnesia_context, :async_dirty)

  def dump() do
    dump(for t <- :kvs.tables(), do: KVS.table(t, :name))
    :ok
  end

  def dump(:short) do
    gen = fn t ->
      {s, m, c} =
        t
        |> Enum.map(&dump_info/1)
        |> unzip3()

      {Enum.uniq(s), Enum.sum(m), Enum.sum(c)}
    end

    dump_format(for t <- :kvs.tables(), do: {KVS.table(t, :name), gen.(KVS.table(t, :name))})
  end

  def dump(table) when is_atom(table), do: dump([table])

  def dump(tables) do
    dump_format(for t <- List.flatten(tables), do: {t, dump_info(t)})
  end

  def dump_info(t) do
    {
      :mnesia.table_info(t, :storage_type),
      :mnesia.table_info(t, :memory) * :erlang.system_info(:wordsize) / 1024 / 1024,
      :mnesia.table_info(t, :size)
    }
  end

  def dump_format(list) do
    :io.format(~c"~20s ~32s ~14s ~10s~n~n", [~c"NAME", ~c"STORAGE TYPE", ~c"MEMORY (MB)", ~c"ELEMENTS"])
    Enum.each(list, fn {t, {s, m, c}} ->
      :io.format(~c"~20s ~32w ~14.2f ~10b~n", [to_charlist(t), s, m, c])
    end)
    :io.format(~c"~nSnapshot taken: ~p~n", [:calendar.now_to_datetime(:os.timestamp())])
  end

  def unzip3([]), do: {[], [], []}
  def unzip3(list) do
    list
    |> Enum.reduce({[], [], []}, fn {a, b, c}, {as, bs, cs} ->
      {[a | as], [b | bs], [c | cs]}
    end)
    |> fn {as, bs, cs} -> {Enum.reverse(as), Enum.reverse(bs), Enum.reverse(cs)} end.()
  end
end
