ExUnit.start()

defmodule Mnesia.Test do
  use ExUnit.Case, async: false
  @moduletag :mnesia
  import Record
  require KVS

  defrecord(:msg, id: [], next: [], prev: [], user: [], msg: [])

  setup do
    orig_dba    = Application.get_env(:kvs, :dba)
    orig_st     = Application.get_env(:kvs, :dba_st)
    orig_schema = Application.get_env(:kvs, :schema)

    Application.put_env(:kvs, :dba,    :kvs_mnesia)
    Application.put_env(:kvs, :dba_st, :kvs_stream)
    Application.put_env(:kvs, :schema, [:kvs, :kvs_stream])

    :kvs.join()

    on_exit(fn ->
      :kvs.leave()
      :kvs.destroy()
      Application.put_env(:kvs, :dba,    orig_dba)
      Application.put_env(:kvs, :dba_st, orig_st)
      Application.put_env(:kvs, :schema, orig_schema)
    end)

    :ok
  end

  setup ctx do
    ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/feed") end, :lists.seq(1, 5))
    {:ok, Map.put(ctx, :ids, ids)}
  end

  defp item_ids(items), do: Enum.map(items, fn m -> msg(m, :id) end)

  test "feed returns all items in insertion order", %{ids: ids} do
    items = :kvs.feed("/feed")
    assert length(items) == 5
    assert ids == item_ids(items)
  end

  test "all returns same as feed", %{ids: _ids} do
    assert :kvs.all("/feed") == :kvs.feed("/feed")
  end

  test "reader uses feed name without double-slash prefix" do
    r = :kvs.reader("/feed")
    assert KVS.reader(feed: "/feed") = r
  end

  test "reader cache is 2-tuple when feed non-empty", %{ids: _ids} do
    r = :kvs.reader("/feed")
    KVS.reader(cache: cache) = r
    assert is_tuple(cache) and tuple_size(cache) == 2
    {tab, _id} = cache
    assert tab == :msg
  end

  test "reader args holds element count when feed non-empty", %{ids: _ids} do
    KVS.reader(args: args) = :kvs.reader("/feed")
    assert args == 5
  end

  test "take returns n items", %{ids: ids} do
    r = :kvs.reader("/feed")
    KVS.reader(args: items) = :kvs.take(KVS.reader(r, args: 3))
    assert length(items) == 3
    assert Enum.take(ids, 3) == item_ids(items)
  end

  test "take more than available returns all", %{ids: ids} do
    r = :kvs.reader("/feed")
    KVS.reader(args: items) = :kvs.take(KVS.reader(r, args: 100))
    assert 5 == length(items)
    assert ids == item_ids(items)
  end

  test "take zero returns empty", %{ids: _ids} do
    r = :kvs.reader("/feed")
    KVS.reader(args: items) = :kvs.take(KVS.reader(r, args: 0))
    assert items == []
  end

  test "take from empty feed returns empty" do
    r = :kvs.reader("/empty")
    KVS.reader(args: items) = :kvs.take(KVS.reader(r, args: 10))
    assert items == []
  end

  test "save and load_reader", %{ids: _ids} do
    r = :kvs.reader("/feed")
    KVS.reader(id: rid) = saved = :kvs.save(r)
    loaded = :kvs.load_reader(rid)
    assert KVS.reader(loaded, :feed) == KVS.reader(saved, :feed)
    assert KVS.reader(loaded, :cache) == KVS.reader(saved, :cache)
  end

  test "take continues from saved position", %{ids: ids} do
    r = :kvs.reader("/feed")
    KVS.reader(id: rid) = :kvs.save(r)

    t1 = KVS.reader(args: a1) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 2))
    assert length(a1) == 2
    assert Enum.take(ids, 2) == item_ids(a1)

    :kvs.save(t1)
    KVS.reader(args: a2) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 3))
    assert length(a2) == 3
    assert Enum.drop(ids, 2) == item_ids(a2)
  end

  test "take in reverse (dir: 1)", %{ids: ids} do
    r = :kvs.top(:kvs.reader("/feed"))
    KVS.reader(args: items) = :kvs.take(KVS.reader(r, args: 3, dir: 1))
    assert length(items) == 3
    assert Enum.take(Enum.reverse(ids), 3) == item_ids(items)
  end

  test "append to writer" do
    :kvs.save(:kvs.add(KVS.writer(:kvs.writer(:sym), args: msg(id: :kvs.seq([], [])))))
    :kvs.save(:kvs.add(KVS.writer(:kvs.writer(:sym), args: msg(id: :kvs.seq([], [])))))
    {:ok, KVS.writer(id: :sym, count: 2)} = :kvs.get(:writer, :sym)
  end

  test "writer count matches appended items", %{ids: _ids} do
    {:ok, KVS.writer(id: "/feed", count: c)} = :kvs.get(:writer, "/feed")
    assert c == 5
  end

  test "feed on separate feeds are independent" do
    a_ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/a") end, :lists.seq(1, 3))
    b_ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/b") end, :lists.seq(1, 4))
    assert 3 == length(:kvs.feed("/a"))
    assert 4 == length(:kvs.feed("/b"))
    assert a_ids == item_ids(:kvs.feed("/a"))
    assert b_ids == item_ids(:kvs.feed("/b"))
  end
end
