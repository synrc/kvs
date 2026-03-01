ExUnit.start()

defmodule Rb.Test do
  use ExUnit.Case, async: false
  require KVS
  import Record

  @moduledoc """
  RocksDB backend test suite.

  ## Running

  Ensure RocksDB is compiled first:

      # Add to mix.exs deps:
      {:rocksdb, "~> 2.5"}

      mix deps.get && mix deps.compile rocksdb
      mix test.rocks
      # or: mix test test/rb_test.exs --only rocks

  All tests carry `@tag :rocks` and are excluded from the default `mix test` run.
  """

  defrecord(:msg, id: [], next: [], prev: [], user: [], msg: [])

  setup do
    # Switch KVS to use RocksDB backend
    Application.put_env(:kvs, :dba, :kvs_rocks)
    Application.put_env(:kvs, :dba_seq, :kvs_rocks)
    Application.put_env(:kvs, :dba_st, :kvs_stream)
    Application.put_env(:kvs, :schema, [:kvs_stream, :kvs_rocks])
    db_name = "test_rocksdb_#{System.unique_integer([:positive])}"
    Application.put_env(:kvs, :rocks_name, db_name)

    :ok = :kvs.join()

    on_exit(fn ->
      :kvs.leave()
      :kvs_rocks.destroy(db_name)
      # Restore default mnesia backend
      Application.put_env(:kvs, :dba, :kvs_mnesia)
      Application.put_env(:kvs, :dba_seq, :kvs_mnesia)
      Application.put_env(:kvs, :schema, [:kvs, :kvs_stream])
    end)

    :ok
  end

  @tag :rocks
  test "basic get/put" do
    id = :kvs.seq([], [])
    m = msg(id: id)
    :kvs.append(m, "/rb/basic")
    {:ok, got} = :kvs.get(:msg, id)
    assert elem(got, 1) == id
  end

  @tag :rocks
  test "append and all" do
    ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/feed") end, :lists.seq(1, 5))
    all = :kvs.all("/rb/feed")
    assert length(all) == 5
    assert Enum.sort(ids) == Enum.sort(Enum.map(all, &elem(&1, 1)))
  end

  @tag :rocks
  test "get_reader and take" do
    ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/take") end, :lists.seq(1, 10))
    r = :kvs.reader("/rb/take")
    assert Record.is_record(r, :reader)
    r1 = :kvs.take(KVS.reader(r, args: 10, dir: 0))
    assert Record.is_record(r1, :reader)
    taken_ids = KVS.reader(r1, :args) |> Enum.map(&elem(&1, 1))
    assert length(taken_ids) == 10
    assert Enum.sort(ids) == Enum.sort(taken_ids)
  end

  @tag :rocks
  test "take partial and resume" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/partial") end, :lists.seq(1, 10))
    r = :kvs.reader("/rb/partial")

    r1 = :kvs.take(KVS.reader(r, args: 3, dir: 0))
    assert Record.is_record(r1, :reader)
    assert length(KVS.reader(r1, :args)) == 3

    r2 = :kvs.take(KVS.reader(r1, args: 7, dir: 0))
    assert Record.is_record(r2, :reader)
    assert length(KVS.reader(r2, :args)) == 7
  end

  @tag :rocks
  test "drop cursor" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/drop") end, :lists.seq(1, 10))
    r = :kvs.save(:kvs.reader("/rb/drop"))
    assert Record.is_record(r, :reader)

    r1 = :kvs.drop(KVS.reader(r, args: 3, dir: 0))
    assert Record.is_record(r1, :reader)

    r2 = :kvs.take(KVS.reader(r1, args: 7, dir: 0))
    assert Record.is_record(r2, :reader)
    assert length(KVS.reader(r2, :args)) == 7
  end

  @tag :rocks
  test "save and load reader" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/save") end, :lists.seq(1, 5))
    r = :kvs.reader("/rb/save")
    KVS.reader(id: rid) = :kvs.save(r)
    rs = :kvs.load_reader(rid)
    assert Record.is_record(rs, :reader)
    assert KVS.reader(rs, :feed) == "/rb/save"
  end

  @tag :rocks
  test "writer count" do
    n = 7
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/writer") end, :lists.seq(1, n))
    w = :kvs.writer("/rb/writer")
    assert Record.is_record(w, :writer)
    assert KVS.writer(w, :count) == n
  end

  @tag :rocks
  test "top and bot" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/topbot") end, :lists.seq(1, 5))
    r = :kvs.reader("/rb/topbot")

    rt = :kvs.top(r)
    assert Record.is_record(rt, :reader)

    rb = :kvs.bot(r)
    assert Record.is_record(rb, :reader)
  end

  @tag :rocks
  test "next traversal" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/next") end, :lists.seq(1, 5))
    KVS.reader(id: rid) = :kvs.save(:kvs.bot(:kvs.reader("/rb/next")))

    Enum.each(1..4, fn _ ->
      cur = :kvs.load_reader(rid)
      r1 = :kvs.next(cur)
      assert Record.is_record(r1, :reader)
      :kvs.save(r1)
    end)

    cur = :kvs.load_reader(rid)
    result = :kvs.next(cur)
    assert match?({:error, _}, result) or Record.is_record(result, :reader)
  end

  @tag :rocks
  test "prev traversal" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/prev") end, :lists.seq(1, 5))
    KVS.reader(id: rid) = :kvs.save(:kvs.top(:kvs.reader("/rb/prev")))

    Enum.each(1..4, fn _ ->
      cur = :kvs.load_reader(rid)
      r1 = :kvs.prev(cur)
      assert Record.is_record(r1, :reader)
      :kvs.save(r1)
    end)

    cur = :kvs.load_reader(rid)
    result = :kvs.prev(cur)
    assert match?({:error, _}, result) or Record.is_record(result, :reader)
  end

  @tag :rocks
  test "bidirectional take-back (dir:1)" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/bidir") end, :lists.seq(1, 10))
    r = :kvs.reader("/rb/bidir")
    KVS.reader(id: rid) = :kvs.save(r)

    forward = KVS.reader(:kvs.take(KVS.reader(:kvs.load_reader(rid), args: 10, dir: 0)), :args)
    assert length(forward) == 10

    backward = KVS.reader(:kvs.take(KVS.reader(:kvs.load_reader(rid), args: 10, dir: 1)), :args)
    assert length(backward) == 10
  end

  @tag :rocks
  test "multiple feeds are independent" do
    ids_a = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/a") end, :lists.seq(1, 5))
    ids_b = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/b") end, :lists.seq(1, 3))

    all_a = :kvs.all("/rb/a")
    all_b = :kvs.all("/rb/b")

    assert length(all_a) == 5
    assert length(all_b) == 3
    assert Enum.sort(ids_a) == Enum.sort(Enum.map(all_a, &elem(&1, 1)))
    assert Enum.sort(ids_b) == Enum.sort(Enum.map(all_b, &elem(&1, 1)))
  end

  @tag :rocks
  test "empty feed returns empty list" do
    assert [] == :kvs.all("/rb/empty")
  end

  @tag :rocks
  test "seq generates unique ids" do
    ids = :lists.map(fn _ -> :kvs.seq([], []) end, :lists.seq(1, 100))
    assert length(Enum.uniq(ids)) == 100
  end

  @tag :rocks
  test "prefix feed helper (:kvs_rocks.fd)" do
    assert :kvs_rocks.fd("/erp/orgs") == "/erp"
    assert :kvs_rocks.fd("/erp/orgs/sub") == "/erp"
    assert :kvs_rocks.fd("/a/b/c") == "/a"
  end

  @tag :rocks
  test "feed aggregation via prefix (RocksDB-exclusive)" do
    duck  = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/agg/duck")  end, :lists.seq(1, 5))
    truck = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/agg/truck") end, :lists.seq(1, 3))

    all_duck  = :kvs.all("/agg/duck")
    all_truck = :kvs.all("/agg/truck")
    all_agg   = :kvs.all("/agg")

    assert length(all_duck)  == 5
    assert length(all_truck) == 3
    assert length(all_agg)   == 8
    assert Enum.sort(all_duck ++ all_truck) == Enum.sort(all_agg)
  end

  @tag :rocks
  test "remove decrements writer count" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/rm") end, :lists.seq(1, 5))
    n = :kvs.remove(:kvs.reader("/rb/rm"))
    assert is_integer(n)
    assert n == 4
  end

  @tag :rocks
  test "corrupted writer cache doesn't break all/feed" do
    :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/rb/corrupt") end, :lists.seq(1, 5))
    prev = :kvs.all("/rb/corrupt")

    w = :kvs.writer("/rb/corrupt")
    w1 = KVS.writer(w, cache: {:msg, "nonexistent"})
    :kvs_rocks.put(w1, :kvs.db())

    after_corrupt = :kvs.all("/rb/corrupt")
    assert length(after_corrupt) == length(prev)
  end
end
