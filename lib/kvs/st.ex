defmodule :kvs_st do
  require KVS
  import KVS, only: [kvs: 1, reader: 0, reader: 1, reader: 2, writer: 0, writer: 1, writer: 2]
  import :kvs_rocks, except: [db: 0]

  def db, do: Application.get_env(:kvs, :rocks_name, "rocksdb")

  def c4(r, v), do: reader(r, args: v)
  def si(m, t), do: put_elem(m, 1, t)
  def id(t), do: elem(t, 1)

  def k(f, []), do: f
  def k(_, {_, id, sf}), do: IO.iodata_to_binary([sf, "/", tb(id)])

  def f2(feed) do
    x = tb(feed)
    case :binary.matches(x, <<"/">>, []) do
      [{0, 1} | _] -> binary_part(x, 1, byte_size(x) - 1)
      _ -> x
    end
  end

  def read_it(c, {:ok, _, [], h}), do: reader(c, cache: [], args: Enum.reverse(h))
  def read_it(c, {:ok, f, v, h}), do: reader(c, cache: {elem(v, 0), id(v), f}, args: Enum.reverse(h))
  def read_it(c, _), do: reader(c, args: [])

  def top(x = reader()), do: top(x, db())
  def top(c = reader(feed: feed), db) do
    writer(count: cn) = get_writer(f2(feed), db)
    read_it(reader(c, count: cn), seek_it(feed, db))
  end

  def bot(x = reader()), do: bot(x, db())
  def bot(c = reader(feed: feed), db) do
    writer(cache: ch, count: cn) = get_writer(f2(feed), db)
    reader(c, cache: ch, count: cn)
  end

  def next(x = reader()), do: next(x, db())
  def next(c = reader(feed: feed, cache: i), db) do
    read_it(c, move_it(k(feed, i), feed, :next, db))
  end

  def prev(x = reader()), do: prev(x, db())
  def prev(c = reader(cache: i, feed: feed), db) do
    read_it(c, move_it(k(feed, i), feed, :prev, db))
  end

  def take(x = reader()), do: take(x, db())
  def take(c = reader(args: n, feed: feed, cache: i, dir: 1), db) do
    read_it(c, take_it(k(feed, i), feed, :prev, n, db))
  end
  def take(c = reader(args: n, feed: feed, cache: i), db) do
    read_it(c, take_it(k(feed, i), feed, :next, n, db))
  end

  def drop(x = reader()), do: drop(x, db())
  def drop(c = reader(args: n), _) when n <= 0, do: c
  def drop(c = reader(), db) do
    reader(take(reader(c, dir: 0), db), args: [])
  end

  def take(_dir, 0, _cache, _reader, acc, _f2, _db), do: acc
  def take(_dir, _n, [], _reader, acc, _f2, _db), do: acc
  def take(1, n, {t, i, _f}, reader(feed: feed), acc, f2, db) do
    case :kvs_rocks.get(t, i, db) do
      {:ok, val} -> take(1, n - 1, :kvs_rocks.prev(val), reader(feed: feed), [val | acc], f2, db)
      _ -> acc
    end
  end
  def take(0, n, {t, i, _f}, reader(feed: feed), acc, f2, db) do
    case :kvs_rocks.get(t, i, db) do
      {:ok, val} -> take(0, n - 1, :kvs_rocks.next(val), reader(feed: feed), [val | acc], f2, db)
      _ -> acc
    end
  end

  def remove(c = reader()), do: remove(c, db())
  def remove(c = reader(feed: feed), db) do
    r = read_it(c, delete_it(feed, db))
    :kvs.delete(:writer, feed)
    r
  end
  def remove(rec, feed), do: remove(rec, feed, db())

  def feed(feed), do: feed(feed, db())
  def feed(feed, db) do
    top = reader(count: cn) = top(get_reader(feed, db), db)
    halt =
      case {estimate(), cn} do
        {e, c} when e <= 0 -> max(c, 4)
        {e, _} -> e
      end
    feed(fn r = reader() -> take(reader(r, args: 4), db) end, top, [], halt)
  end

  defp feed(_f, reader(), acc, h) when h <= 0, do: acc
  defp feed(f, r = reader(cache: c1, feed: feed_id), acc, h) do
    r1 = reader(args: a, cache: ch) = f.(r)

    cond do
      ch == c1 ->
        acc ++ a

      tuple_size(ch) == 3 ->
        {_, _, k} = ch
        if binary_part(k, 0, byte_size(feed_id)) == feed_id and length(a) == 4 do
          feed(f, r1, acc ++ a, h - 4)
        else
          acc ++ a
        end

      true ->
        acc ++ a
    end
  end

  def load_reader(id), do: load_reader(id, db())
  def load_reader(id, db) do
    case :kvs.get(:reader, id, kvs(db: db, mod: :kvs_rocks)) do
      {:ok, c = reader()} -> c
      _ -> reader(id: :kvs.seq([], []))
    end
  end

  def get_writer(id), do: get_writer(id, db())
  def get_writer(id, db) do
    case :kvs.get(:writer, id, kvs(db: db, mod: :kvs_rocks)) do
      {:ok, w} -> w
      {:error, _} -> writer(id: id)
    end
  end

  def get_reader(id), do: get_reader(id, db())
  def get_reader(id, db) do
    case :kvs.get(:writer, id, kvs(db: db, mod: :kvs_rocks)) do
      {:ok, writer(id: feed, count: cn, cache: ch)} ->
        read_it(reader(id: :kvs.seq([], []), feed: key(feed), count: cn, cache: ch), seek_it(key(feed), db))
      {:error, _} ->
        read_it(reader(id: :kvs.seq([], []), feed: key(id), count: 0, cache: []), seek_it(key(id), db))
    end
  end

  def save(c), do: save(c, db())
  def save(c, db) when elem(c, 0) == :reader do
    n1 = case id(c) do
      [] -> si(c, :kvs.seq([], []))
      _ -> c
    end
    nc = c4(n1, [])
    :kvs.put(nc, kvs(db: db, mod: :kvs_rocks))
    nc
  end
  def save(c, db) when elem(c, 0) == :writer do
    :kvs.put(c, kvs(db: db, mod: :kvs_rocks))
    c
  end

  def raw_append(m, feed), do: raw_append(m, feed, db())
  def raw_append(m, feed, db) do
    :rocksdb.put(ref(db), key(feed, elem(m, 1)), :erlang.term_to_binary(m), sync: true)
  end

  def add(x = writer()), do: add(x, db())
  def add(c = writer(args: m), db) when elem(m, 1) == [], do: add(si(m, :kvs.seq([], [])), c, db)
  def add(c = writer(args: m), db), do: add(m, c, db)

  def add(m, c = writer(id: feed, count: s), db) do
    ns = s + 1
    raw_append(m, feed, db)
    writer(c, cache: {elem(m, 0), elem(m, 1), key(feed)}, count: ns)
  end

  def cut(feed), do: cut(feed, db())
  def cut(feed, db) do
    writer(cache: {_, key, fd} = ch) = :kvs.get_writer(feed, kvs(db: db, mod: :kvs_rocks))
    reader() = :kvs.prev(get_reader(feed, db))
    reader() = :kvs.next(reader(feed: key(feed), cache: ch))

    :kvs.delete_range(feed, {fd, key}, kvs(db: db, mod: :kvs_rocks))
  end

  def remove(rec, feed, db) do
    :kvs.ensure(writer(id: feed), kvs(db: db, mod: :kvs_rocks))
    w = writer(count: c, cache: ch) = :kvs.get_writer(feed, kvs(db: db, mod: :kvs_rocks))

    ch1 =
      case {elem(rec, 0), elem(rec, 1), key(feed)} do
        ^ch ->
          r = get_reader(feed, db)
          elem(prev(reader(r, cache: ch), db), 3) # elem(_, 3) == e(4, ...)
        _ ->
          ch
      end

    case :kvs.delete(feed, id(rec), kvs(db: db, mod: :kvs_rocks)) do
      :ok ->
        count = c - 1
        save(writer(w, count: count, cache: ch1), db)
        count

      _ ->
        c
    end
  end

  def append(rec, feed), do: append(rec, feed, db())
  def append(rec, feed, db) do
    :kvs.ensure(writer(id: feed), kvs(db: db, mod: :kvs_rocks))
    id = elem(rec, 1)
    w = get_writer(feed, db)

    case :kvs.get(feed, id, kvs(db: db, mod: :kvs_rocks)) do
      {:ok, _} ->
        raw_append(rec, feed, db)
        id

      {:error, _} ->
        save(add(writer(w, args: rec), db), db)
        id
    end
  end
end
