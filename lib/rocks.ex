defmodule :kvs_rocks do
  require KVS

  def e(x, y), do: elem(x, y)

  def bt([]), do: []
  def bt(x), do: :erlang.binary_to_term(x)

  def tb([]), do: <<>>
  def tb(t) when is_list(t), do: :unicode.characters_to_nfkc_binary(t)
  def tb(t) when is_atom(t), do: :erlang.atom_to_binary(t, :utf8)
  def tb(t) when is_binary(t), do: t
  def tb(t), do: :erlang.term_to_binary(t)

  def sz([]), do: 0
  def sz(b), do: byte_size(b)

  def key(r) when is_tuple(r) and tuple_size(r) > 1, do: key(elem(r, 0), elem(r, 1))
  def key(feed) do
    x = tb(feed)
    case :binary.matches(x, [<<"/">>]) do
      [{0, 1} | _] -> <<"/", x::binary>>
      _ -> <<"/", x::binary>>
    end
  end

  def key(:writer, r) do
    IO.iodata_to_binary([Enum.join(["", :erlang.atom_to_binary(:writer, :utf8), tb(r)], <<"/">>)])
  end

  def key(tab, r) do
    fd = if tab == [], do: "", else: tb(tab)
    IO.iodata_to_binary([Enum.join(["", fd, fmt(r)], <<"/">>)])
  end

  def keys(tab, db) do
    feed = key(tab, [])
    {:ok, h} = :rocksdb.iterator(ref(db), [])

    keys_loop = fn
      loop, k1, acc ->
        if String.starts_with?(k1, feed) do
          case :rocksdb.iterator_move(h, :next) do
            {:ok, k2, _} -> loop.(loop, k2, [tb(k1) | acc])
            _ -> Enum.reverse([tb(k1) | acc])
          end
        else
          :rocksdb.iterator_close(h)
          Enum.reverse(acc)
        end
    end

    {:ok, k, _} = :rocksdb.iterator_move(h, {:seek, feed})
    keys_loop.(keys_loop, k, [])
  end

  def key_match(tab, id, db) do
    feed = key(tab, [])
    {:ok, h} = :rocksdb.iterator(ref(db), [])

    keys_loop = fn
      loop, k1 ->
        cond do
          String.starts_with?(k1, feed) and String.ends_with?(k1, id) ->
            :rocksdb.iterator_close(h)
            [k1]

          String.starts_with?(k1, feed) ->
            case :rocksdb.iterator_move(h, :next) do
              {:ok, k2, _} -> loop.(loop, k2)
              _ -> []
            end

          true ->
            :rocksdb.iterator_close(h)
            []
        end
    end

    {:ok, k, _} = :rocksdb.iterator_move(h, {:seek, feed})
    keys_loop.(keys_loop, k)
  end

  def fmt([]), do: []
  def fmt(k) do
    key = tb(k)
    end_sz = sz(key)

    {s, e} =
      case :binary.matches(key, [<<"/">>]) do
        [{0, 1}] -> {1, end_sz - 1}
        [{0, 1}, {1, 1}] -> {2, end_sz - 2}
        [{0, 1}, {1, 1} | _] -> {2, end_sz - 2}
        [{0, 1} | _] -> {1, end_sz - 1}
        _ -> {0, end_sz}
      end

    :binary.part(key, s, e)
  end

  def fd(k) do
    key = tb(k)
    end_sz = sz(key)

    {s, _} =
      case :binary.matches(key, [<<"/">>]) do
        [{0, 1}] -> {end_sz, end_sz}
        [{0, 1}, {1, 1}] -> {end_sz, end_sz}
        [{0, 1}, {1, 1} | t] -> hd(Enum.reverse(t))
        [{0, 1} | t] -> hd(Enum.reverse(t))
        _ -> {end_sz, end_sz}
      end

    :binary.part(key, 0, s)
  end

  def run(<<>>, sk, _, _, _), do: {:ok, sk, [], []}

  def run(key, sk, dir, compiled_operations, db) do
    s = sz(sk)
    initial_object = {ref(db), []}

    run_fn = fn
      f, k, h, v, acc when is_binary(k) ->
        if :binary.part(k, 0, min(byte_size(k), s)) == sk do
          {f.(h, dir), h, [v | acc]}
        else
          stop_it(h)
          throw({:ok, fd(k), bt(v), for(a1 <- acc, do: bt(a1))})
        end
    end

    range_check = fn f, k, h, v ->
      case f.(h, :prev) do
        {:ok, k1, v1} ->
          if :binary.part(k, 0, min(byte_size(k), s)) == sk do
            {{:ok, k1, v1}, h, [v]}
          else
            run_fn.(f, k1, h, v1, [])
          end

        _ ->
          stop_it(h)
          throw({:ok, fd(k), bt(v), [bt(v)]})
      end
    end

    state_machine = fn
      el, obj ->
        f = el
        case obj do
          {:ok, h} -> {f.(h, {:seek, key}), h}
          {{:ok, k, v}, h} when dir == :prev -> range_check.(f, k, h, v)
          {{:ok, k, v}, h} -> run_fn.(f, k, h, v, [])
          {{:ok, k, v}, h, a} -> run_fn.(f, k, h, v, a)
          {{:error, e}, h, acc} -> {{:error, e}, h, acc}
          {i, o} -> f.(i, o)
        end
    end

    try do
      case Enum.reduce(compiled_operations, initial_object, state_machine) do
        {{:ok, k, bin}, h, a} ->
          stop_it(h)
          {:ok, fd(k), bt(bin), for(a1 <- a, do: bt(a1))}

        {{:ok, k, bin}, h} ->
          stop_it(h)
          {:ok, fd(k), bt(bin), []}

        {{:error, _}, h, acc} ->
          stop_it(h)
          {:ok, fd(sk), bt(shd(acc)), for(a1 <- acc, do: bt(a1))}

        {{:error, _}, h} ->
          stop_it(h)
          {:ok, fd(sk), [], []}
      end
    catch
      {:ok, k, v, acc} -> {:ok, k, v, acc}
    end
  end

  def initialize, do: for(m <- :kvs.modules(), do: :kvs.initialize(:kvs_rocks, m))
  def index(_, _, _), do: []

  def ref_env(db), do: String.to_atom("rocks_ref_" <> to_string(db))
  def db, do: Application.get_env(:kvs, :rocks_name, "rocksdb")
  def start, do: :ok
  def stop, do: :ok
  def destroy, do: destroy(db())
  def destroy(db), do: :rocksdb.destroy(db, [])
  def version, do: {:version, "KVS ROCKSDB"}
  def dir, do: []
  def match(_), do: []
  def index_match(_, _), do: []
  def ref, do: ref(db())
  def ref(db), do: Application.get_env(:kvs, ref_env(db), [])
  def leave, do: leave(db())

  def leave(db) do
    case ref(db) do
      [] -> :skip
      x ->
        :rocksdb.close(x)
        Application.put_env(:kvs, ref_env(db), [])
        :ok
    end
  end

  def join(_, db) do
    Application.start(:rocksdb)
    leave(db)
    {:ok, r} = :rocksdb.open(db, create_if_missing: true)
    initialize()
    Application.put_env(:kvs, ref_env(db), r)
  end

  def compile(:it), do: [&:rocksdb.iterator/2]
  def compile(:seek), do: [&:rocksdb.iterator/2, &:rocksdb.iterator_move/2]
  def compile(:move), do: [&:rocksdb.iterator_move/2]
  def compile(:close), do: [&:rocksdb.iterator_close/1]
  def compile(:take, n), do: for(_ <- 1..n, do: &:rocksdb.iterator_move/2)

  def compile(:delete, _, {:error, e}, _), do: {:error, e}

  def compile(:delete, sk, {:ok, _, v1, _}, db) do
    f1 = key(key(fmt(sk), elem(v1, 1)))
    s = sz(sk)

    del = fn
      del_loop, h, dir ->
        case :rocksdb.delete(ref(db), f1, []) do
          :ok ->
            case :rocksdb.iterator_move(h, dir) do
              {:ok, k, _} ->
                if :binary.part(k, 0, min(byte_size(k), s)) == sk do
                  if :rocksdb.delete(ref(db), k, []) == :ok, do: del_loop.(del_loop, h, dir), else: :error
                else
                  {:ok, k, []}
                end

              {:ok, k} ->
                if :binary.part(k, 0, min(byte_size(k), s)) == sk do
                  if :rocksdb.delete(ref(db), k, []) == :ok, do: del_loop.(del_loop, h, dir), else: :error
                else
                  {:ok, k}
                end

              e ->
                e
            end

          e ->
            e
        end
    end

    [fn h, dir -> del.(del, h, dir) end]
  end

  def stop_it(h) do
    try do
      [f] = compile(:close)
      f.(h)
    catch
      :error, :badarg -> :ok
    end
  end

  def seek_it(k), do: seek_it(k, db())
  def seek_it(k, db), do: run(k, k, :ok, compile(:seek), db)

  def move_it(key, sk, dir), do: move_it(key, sk, dir, db())
  def move_it(key, sk, dir, db), do: run(key, sk, dir, compile(:seek) ++ compile(:move), db)

  def take_it(key, sk, dir, n), do: take_it(key, sk, dir, n, db())

  def take_it(key, sk, dir, n, db) when is_integer(n) and n >= 0 do
    run(key, sk, dir, compile(:seek) ++ compile(:take, n), db)
  end

  def take_it(key, sk, dir, _, db), do: take_it(key, sk, dir, 0, db)

  def delete_it(fd), do: delete_it(fd, db())

  def delete_it(fd, db) do
    run(fd, fd, :next, compile(:seek) ++ compile(:delete, fd, seek_it(fd), db), db)
  end

  def all(r, db), do: :kvs_st.feed(r, db)

  def get(tab, {:step, n, [208 | _] = key}, db), do: get(tab, {:step, n, :erlang.list_to_binary(key)}, db)
  def get(tab, [208 | _] = key, db), do: get(tab, :erlang.list_to_binary(key), db)

  def get(tab, key, db) do
    case :rocksdb.get(ref(db), key(tab, key), []) do
      :not_found -> {:error, :not_found}
      {:ok, bin} -> {:ok, bt(bin)}
    end
  end

  def put(record), do: :kvs_rocks.put(record, db())

  def put(records, db) when is_list(records) do
    Enum.map(records, &:kvs_rocks.put(&1, db))
  end

  def put(record, db) do
    :rocksdb.put(ref(db), key(record), :erlang.term_to_binary(record), sync: true)
  end

  def delete(feed, id, db), do: :rocksdb.delete(ref(db), key(feed, id), [])

  def delete_range(feed, {fd, key}, db) do
    last = key(key(fmt(fd), key))
    read_ops = [prefix_same_as_start: true]
    compact_ops = [change_level: true]
    feed1 = key(feed)
    sz_feed = byte_size(feed1)

    reopen =
      case ref(db) do
        [] -> :skip
        _ ->
          leave(db)
          :ok
      end

    {:ok, r} = :rocksdb.open(db, prefix_extractor: {:capped_prefix_transform, sz_feed})
    {:ok, h} = :rocksdb.iterator(r, read_ops)
    {:ok, start, _} = :rocksdb.iterator_move(h, {:seek, feed1})

    :ok = :rocksdb.delete_range(r, start, last, [])
    :ok = :rocksdb.delete(r, last, [])
    :ok = :rocksdb.delete(r, key(:writer, feed), [])
    :ok = :rocksdb.compact_range(r, start, :undefined, compact_ops)
    :ok = :rocksdb.iterator_close(h)
    :ok = :rocksdb.close(r)

    if reopen == :ok, do: join([], db), else: :ok
  end

  def count(_), do: 0

  def estimate, do: estimate(db())

  def estimate(db) do
    case :rocksdb.get_property(ref(db), <<"rocksdb.estimate-num-keys">>) do
      {:ok, est} when is_binary(est) -> String.to_integer(est)
      {:ok, est} when is_list(est) -> List.to_integer(est)
      {:ok, est} when is_integer(est) -> est
      _ -> 0
    end
  end

  def shd([]), do: []
  def shd(x), do: hd(x)

  def create_table(_, _), do: []
  def add_table_index(_, _), do: :ok
  def dump, do: :ok

  def seq(_, _) do
    val =
      case :os.type() do
        {:win32, :nt} ->
          {mega, sec, micro} = :erlang.timestamp()
          Integer.to_string((mega * 1_000_000 + sec) * 1_000_000 + micro)

        _ ->
          Integer.to_string(elem(hd(Enum.reverse(:erlang.system_info(:os_system_time_source))), 1))
      end

    pad = 20 - byte_size(val)

    if pad > 0 do
      String.duplicate("0", pad) <> val
    else
      val
    end
  end
end
