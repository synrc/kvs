defmodule :kvs_stream do
  require KVS

  def db, do: :kvs.db()

  def metainfo, do: KVS.schema(name: :kvs, tables: tables())

  def tables do
    [
      KVS.table(name: :writer, fields: [:id, :count, :args, :cache, :first]),
      KVS.table(name: :reader, fields: [:id, :pos, :cache, :args, :feed, :seek, :count, :dir])
    ]
  end

  def c4(r, v), do: KVS.reader(r, args: v)
  def sn(m, t), do: put_elem(m, 2, t)
  def sp(m, t), do: put_elem(m, 3, t)
  def si(m, t), do: put_elem(m, 1, t)
  def tab(w) when elem(w, 0) == :writer, do: :msg
  def tab(m) when elem(m, 0) == :msg, do: :msg
  def tab(t) when is_tuple(t), do: elem(t, 0)
  def tab(_), do: []
  def id(t) when is_tuple(t), do: elem(t, 1) # iter :id
  def id(t), do: t
  def en(t) when is_tuple(t), do: elem(t, 2) # iter :next
  def en(_), do: []
  def ep(t) when is_tuple(t), do: elem(t, 3) # iter :prev
  def ep(_), do: []
  def acc(0), do: :next
  def acc(1), do: :prev

  def n({:ok, r}, c, p, db), do: r(:kvs.get(tab(r), en(r), KVS.kvs(db: db)), c, p)
  def n({:error, x}, _, _, _), do: {:error, x}

  def p({:ok, r}, c, p, db), do: r(:kvs.get(tab(r), ep(r), KVS.kvs(db: db)), c, p)
  def p({:error, x}, _, _, _), do: {:error, x}

  def r({:ok, val}, c, p), do: KVS.reader(c, cache: {tab(val), id(val)}, pos: p, args: List.flatten([val]))
  def r({:error, x}, _, _), do: {:error, x}

  def w({:ok, _w = KVS.writer(first: [])}, :bot, c), do: KVS.reader(c, cache: [], pos: 1, dir: 0)
  def w({:ok, w = KVS.writer(first: b)}, :bot, c), do: KVS.reader(c, cache: {tab(w), id(b)}, pos: 1, dir: 0)
  def w({:ok, w = KVS.writer(cache: b, count: size)}, :top, c), do: KVS.reader(c, cache: {tab(w), id(b)}, pos: size, dir: 1)
  def w({:error, x}, _, _), do: {:error, x}

  def top(x = KVS.reader()), do: top(x, db())
  def top(c = KVS.reader(feed: f), db), do: w(:kvs.get(:writer, f, KVS.kvs(db: db)), :top, c)

  def bot(x = KVS.reader()), do: bot(x, db())
  def bot(c = KVS.reader(feed: f), db), do: w(:kvs.get(:writer, f, KVS.kvs(db: db)), :bot, c)

  def next(x = KVS.reader()), do: next(x, db())
  def next(KVS.reader(cache: []), _), do: {:error, :empty}
  def next(c = KVS.reader(cache: {t, r}, pos: p), db), do: n(:kvs.get(t, r, KVS.kvs(db: db)), c, p + 1, db)

  def prev(x = KVS.reader()), do: prev(x, db())
  def prev(KVS.reader(cache: []), _), do: {:error, :empty}
  def prev(c = KVS.reader(cache: {t, r}, pos: p), db), do: p(:kvs.get(t, r, KVS.kvs(db: db)), c, p - 1, db)

  def drop(x = KVS.reader()), do: drop(x, db())
  def drop(c = KVS.reader(cache: []), _), do: KVS.reader(c, args: [])
  def drop(c = KVS.reader(dir: d, cache: b, args: n, pos: p), db), do: drop(acc(d), n, c, c, p, b, db)

  def take(x = KVS.reader()), do: take(x, db())
  def take(c = KVS.reader(cache: []), _), do: KVS.reader(c, args: [])
  def take(c = KVS.reader(dir: d, cache: _b, args: n, pos: p), db), do: take(acc(d), n, c, c, [], p, db)

  def take(_, _, {:error, _}, c2, r, p, _) do
    if r == [] do
      KVS.reader(c2, args: [], pos: p, cache: [])
    else
      flat = List.flatten(r)
      rev = Enum.reverse(flat)
      KVS.reader(c2, args: rev, pos: p, cache: {tab(hd(flat)), id(hd(flat))})
    end
  end

  def take(_, 0, c, c2, r, p, _) when elem(c, 0) == :reader do
    flat = List.flatten([r])
    case flat do
      [] -> KVS.reader(c2, args: [], pos: p, cache: [])
      _ -> KVS.reader(c2, args: Enum.reverse(flat), pos: p, cache: KVS.reader(c, :cache))
    end
  end
  def take(_, 0, {:error, _}, c2, r, p, _) do
    flat = List.flatten([r])
    case flat do
      [] -> KVS.reader(c2, args: [], pos: p, cache: [])
      _ -> KVS.reader(c2, args: Enum.reverse(flat), pos: p, cache: {tab(hd(flat)), id(hd(flat))})
    end
  end
  def take(_, 0, KVS.reader(cache: []), c2, r, p, _) do
    flat = List.flatten([r])
    case flat do
      [] -> KVS.reader(c2, args: [], pos: p, cache: [])
      _ -> KVS.reader(c2, args: Enum.reverse(flat), pos: p, cache: {tab(hd(flat)), id(hd(flat))})
    end
  end

  def take(a, n, c = KVS.reader(cache: {t, i}, pos: p), c2, r, _, db) do
    {:ok, val} = :kvs.get(t, i, KVS.kvs(db: db))
    next_c = Kernel.apply(__MODULE__, a, [c, db])
    case next_c do
      KVS.reader(cache: []) -> take(0, n - 1, next_c, c2, [val | r], p, db)
      {:error, _} -> take(0, n - 1, next_c, c2, [val | r], p, db)
      {:ok, _} -> take(a, n - 1, next_c, c2, [val | r], p, db)
      _ -> take(a, n - 1, next_c, c2, [val | r], p, db)
    end
  end

  def drop(_, _, {:error, _}, c2, p, b, _), do: KVS.reader(c2, pos: p, cache: b)
  def drop(_, 0, _, c2, p, b, _), do: KVS.reader(c2, pos: p, cache: b)
  def drop(a, n, c = KVS.reader(cache: _b, pos: p), c2, _, _, db) do
    next_c = Kernel.apply(__MODULE__, a, [c, db])
    next_b = case next_c do
      KVS.reader(cache: new_b) -> new_b
      _ -> []
    end
    drop(a, n - 1, next_c, c2, p, next_b, db)
  end

  def feed(feed), do: feed(feed, db())
  def feed(feed, db) do
    KVS.reader(args: args) = take(KVS.reader(get_reader(feed, db), args: -1), db)
    args
  end

  def load_reader(id), do: load_reader(id, db())
  def load_reader(id, db) do
    case :kvs.get(:reader, id, KVS.kvs(db: db)) do
      {:ok, c} -> c
      _ -> KVS.reader(id: [])
    end
  end

  def save(c), do: save(c, db())
  def save(c, db) when elem(c, 0) == :reader do
    nc = c4(c, [])
    :kvs.put(nc, KVS.kvs(db: db))
    nc
  end
  def save(c, db) when elem(c, 0) == :writer do
    :kvs.put(c, KVS.kvs(db: db))
    c
  end

  def get_writer(id), do: get_writer(id, db())
  def get_writer(id, db) do
    case :kvs.get(:writer, id, KVS.kvs(db: db)) do
      {:ok, w} -> w
      {:error, _} -> KVS.writer(id: id)
    end
  end

  def get_reader(id), do: get_reader(id, db())
  def get_reader(id, db) do
    case :kvs.get(:writer, id, KVS.kvs(db: db)) do
      {:ok, KVS.writer(first: [], count: c)} -> KVS.reader(id: :kvs.seq(:reader, 1, KVS.kvs(db: db)), feed: id, args: c, cache: [])
      {:ok, KVS.writer(first: f, count: c)} -> KVS.reader(id: :kvs.seq(:reader, 1, KVS.kvs(db: db)), feed: id, args: c, cache: {tab(f), id(f)})
      {:error, _} ->
        save(KVS.writer(id: id), db)
        get_reader(id, db)
    end
  end

  def add(x = KVS.writer()), do: add(x, db())
  def add(c = KVS.writer(args: m), db) when elem(m, 1) == [], do: add(si(m, :kvs.seq(tab(m), 1)), c, db)
  def add(c = KVS.writer(args: m), db), do: add(m, c, db)

  def add(m, c = KVS.writer(cache: []), db) do
    _id = id(m)
    n = sp(sn(m, []), [])
    :kvs.put(n, KVS.kvs(db: db))
    KVS.writer(c, cache: n, count: 1, first: n)
  end

  def add(m, c = KVS.writer(cache: v1, count: s), db) do
    {:ok, v} = :kvs.get(tab(v1), id(v1), KVS.kvs(db: db))
    n = sp(sn(m, []), id(v))
    p = sn(v, id(m))
    :kvs.put([n, p], KVS.kvs(db: db))
    KVS.writer(c, cache: n, count: s + 1)
  end

  def cut(feed), do: cut(feed, db())
  def cut(_, _), do: :ignore

  def remove(c = KVS.reader(feed: feed)), do: remove(c, feed, db())
  def remove(rec, feed), do: remove(rec, feed, db())
  def remove(_rec, feed, db) do
    case :kvs.get(:writer, feed, KVS.kvs(db: db)) do
      {:ok, w = KVS.writer(count: count)} ->
        :kvs.save(KVS.writer(w, count: count - 1), KVS.kvs(db: db))
        count - 1
      {:error, _} -> {:error, :not_found}
    end
  end

  def append(rec, feed), do: append(rec, feed, db())
  def append(rec, feed, db) do
    :kvs.ensure(KVS.writer(id: feed), KVS.kvs(db: db))
    id = elem(rec, 1)
    name = elem(rec, 0)

    case :kvs.get(name, id, KVS.kvs(db: db)) do
      {:ok, _msg} ->
        id

      {:error, _} ->
        {:ok, w = KVS.writer(first: first, cache: c, count: count)} = :kvs.get(:writer, feed, KVS.kvs(db: db))
        prev = if c == [], do: [], else: elem(c, 1)

        if prev != [] do
          {:ok, msg} = :kvs.get(name, prev, KVS.kvs(db: db))
          :kvs.put(:kvs.setfield(msg, :next, id), KVS.kvs(db: db))
        end

        rec_with_prev = :kvs.setfield(rec, :prev, prev)
        :kvs.put(rec_with_prev, KVS.kvs(db: db))
        nc = count + 1

        w_new =
          if first == [] do
            KVS.writer(w, count: nc, first: {name, id}, cache: {name, id})
          else
            KVS.writer(w, count: nc, cache: {name, id})
          end
        save(w_new, db)
        id
    end
  end
end
