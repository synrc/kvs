ExUnit.start()

defmodule Fs.Test do
  use ExUnit.Case, async: false
  @moduletag :mnesia
  import Record
  require KVS

  defrecord(:msg, id: [], next: [], prev: [], user: [], msg: [])

  setup do
    orig_dba    = Application.get_env(:kvs, :dba)
    orig_st     = Application.get_env(:kvs, :dba_st)
    orig_schema = Application.get_env(:kvs, :schema)

    Application.put_env(:kvs, :schema, [:kvs, :kvs_stream])

    # Start mnesia for seq support (kvs_fs.seq delegates to kvs_mnesia)
    :kvs_mnesia.join([], [])

    # Switch to FS for data storage, then init FS dirs
    Application.put_env(:kvs, :dba,    :kvs_fs)
    Application.put_env(:kvs, :dba_st, :kvs_stream)
    :kvs_fs.join([], [])

    on_exit(fn ->
      :mnesia.stop()
      :mnesia.delete_schema([node()])
      File.rm_rf!("data")
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

  test "put and get record" do
    record = msg(id: "test-key", msg: "hello")
    :kvs.put(record)
    assert {:ok, stored} = :kvs.get(:msg, "test-key")
    assert msg(stored, :id) == "test-key"
    assert msg(stored, :msg) == "hello"
  end

  test "get missing record returns error" do
    assert {:error, _} = :kvs.get(:msg, "nonexistent")
  end

  test "delete record" do
    record = msg(id: "del-key")
    :kvs.put(record)
    assert {:ok, _} = :kvs.get(:msg, "del-key")
    :kvs.delete(:msg, "del-key")
    assert {:error, _} = :kvs.get(:msg, "del-key")
  end

  test "put list of records" do
    records = [msg(id: "k1"), msg(id: "k2"), msg(id: "k3")]
    :kvs.put(records)
    assert {:ok, r1} = :kvs.get(:msg, "k1")
    assert {:ok, r2} = :kvs.get(:msg, "k2")
    assert {:ok, r3} = :kvs.get(:msg, "k3")
    assert msg(r1, :id) == "k1"
    assert msg(r2, :id) == "k2"
    assert msg(r3, :id) == "k3"
  end

  test "feed returns all items in insertion order", %{ids: ids} do
    items = :kvs.feed("/feed")
    assert length(items) == 5
    assert ids == item_ids(items)
  end

  test "all returns same as feed", %{ids: _ids} do
    assert :kvs.all("/feed") == :kvs.feed("/feed")
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

  test "take from empty feed returns empty" do
    r = :kvs.reader("/empty-fs")
    KVS.reader(args: items) = :kvs.take(KVS.reader(r, args: 10))
    assert items == []
  end

  test "writer count matches appended items", %{ids: _ids} do
    w = :kvs.writer("/feed")
    assert KVS.writer(w, :count) == 5
  end

  test "save and load_reader", %{ids: _ids} do
    r = :kvs.reader("/feed")
    KVS.reader(id: rid) = saved = :kvs.save(r)
    loaded = :kvs.load_reader(rid)
    assert KVS.reader(loaded, :feed) == KVS.reader(saved, :feed)
    assert KVS.reader(loaded, :cache) == KVS.reader(saved, :cache)
  end

  test "independent feeds", %{ids: _ids} do
    a_ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/alpha") end, :lists.seq(1, 3))
    b_ids = :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([], [])), "/beta") end, :lists.seq(1, 2))
    assert 3 == length(:kvs.feed("/alpha"))
    assert 2 == length(:kvs.feed("/beta"))
    assert a_ids == item_ids(:kvs.feed("/alpha"))
    assert b_ids == item_ids(:kvs.feed("/beta"))
  end

  test "data directory created on join" do
    assert File.dir?("data")
  end
end
