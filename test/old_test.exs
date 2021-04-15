ExUnit.start()

defmodule OLD.Test do
  use ExUnit.Case, async: true
  require KVS

  setup do: (on_exit(fn -> :ok = :kvs.leave();:ok = :kvs.destroy() end);:kvs.join())

  test "basic" do
    id1 = {:basic, :kvs.seq([], [])}
    id2 = {:basic, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id1))
    :kvs.save(:kvs.writer(id2))

    :lists.map(
      fn _ ->
        :kvs.save(:kvs.add(KVS.writer(:kvs.writer(id1), args: {:"$msg", [], [], [], [], []})))
      end,
      :lists.seq(1, x)
    )

    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id2) end, :lists.seq(1, x))
    r1 = :kvs.save(:kvs.reader(id1))
    r2 = :kvs.save(:kvs.reader(id2))
    x1 = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r1, :id)), args: 20))
    x2 = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r2, :id)), args: 20))
    b = KVS.reader(:kvs.feed(id1), :args)

    case :application.get_env(:kvs, :dba_st, :kvs_st) do
      :kvs_st ->
        c = :kvs.all(id2)
        assert :lists.reverse(c) == KVS.reader(x2, :args)

      _ ->
        # mnesia doesn't support `all` over feeds (only for tables)
        []
    end

    assert KVS.reader(x1, :args) == b

    assert length(KVS.reader(x1, :args)) == length(KVS.reader(x2, :args))
    assert x == length(b)
  end

  test "sym" do
    id = {:sym, :kvs.seq([], [])}
    :kvs.save(:kvs.writer(id))
    x = 5

    :lists.map(
      fn
        z ->
          :kvs.remove(KVS.writer(z, :cache), id)
      end, :lists.map(
        fn _ ->
          :kvs.save(:kvs.add(KVS.writer(:kvs.writer(id), args: {:"$msg", [], [], [], [], []})))
        end,
        :lists.seq(1, x)
      )
    )

    {:ok, KVS.writer(count: 0)} = :kvs.get(:writer, id)
  end

  test "take" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    KVS.reader(id: rid) = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 20))
    b = KVS.reader(:kvs.feed(id), :args)
    #: mnesia
    assert KVS.reader(t, :args) == b
  end

  test "take back full" do
    log(:st, "take back full")
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    KVS.reader(id: rid) = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 5))
    :kvs.save(KVS.reader(t, dir: 1))
    log("t:", t)
    n = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 5))
    b = KVS.reader(:kvs.feed(id), :args)
    log("n:", n)
    assert KVS.reader(n, :args) == KVS.reader(t, :args)
    assert KVS.reader(t, :args) == b
    log(:end, "take back full")
  end

  test "partial take back" do
    id = {:partial, :kvs.seq([], [])}
    x = 3
    p = 2
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    KVS.reader(id: rid) = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    :kvs.save(KVS.reader(t, dir: 1))
    n = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p + 1))
    assert KVS.reader(t, :args) == tl(KVS.reader(n, :args))
  end

  test "partial full bidirectional" do
    log(:st, "partial full bidirectional")
    id = {:partial, :kvs.seq([], [])}
    x = 5
    p =2
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", :kvs.seq([],[]), [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    t1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z1 = KVS.reader(t1, :args)
    IO.inspect :kvs.all(id)
    r = :kvs.save(t1)
    log("next t1:", t1)

    t2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z2 = KVS.reader(t2, :args)
    r = :kvs.save(t2)
    log("next t2:", t2)

    t3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z3 = KVS.reader(t3, :args)
    :kvs.save(KVS.reader(t3, dir: 1, pos: 0))
    log("next t3:", t3)

    n1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    nz1 = KVS.reader(n1, :args)
    :kvs.save n1
    log("prev n1:", n1)

    n2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    nz2 = KVS.reader(n2, :args)
    :kvs.save n2
    log("prev n2:", n2)

    n3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    nz3 = KVS.reader(n3, :args)
    log("prev n3:", n3)

    assert z3 ++ z2 ++ z1 == nz1 ++ nz2 ++ nz3
    log(:end, "partial full bidirectional")
  end

  test "test bidirectional (new)" do
    log(:st, "test bidirectional (new)")
    id = {:partial, :kvs.seq([], [])}
    x = 6
    p = 3
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", :kvs.seq([],[]), [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    IO.inspect :kvs.all(id)

    t1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z1 = KVS.reader(t1, :args)
    r = :kvs.save(t1)
    log("next t1:", t1)

    t2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z2 = KVS.reader(t2, :args)
    r = :kvs.save(t2)
    log("next t2:", t2)

    t3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z3 = KVS.reader(t3, :args)
    :kvs.save(KVS.reader(t3, dir: 1, pos: 0))
    log("next t3:", t3)

    assert z3 == []

    n1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    nz1 = KVS.reader(n1, :args)
    :kvs.save n1
    log("prev n1:", n1)

    n2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    nz2 = KVS.reader(n2, :args)
    :kvs.save n2
    log("prev n2:", n2)

    n3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    nz3 = KVS.reader(n3, :args)
    :kvs.save(KVS.reader(n3, dir: 0))
    log("prev n3:", n3)

    assert nz3 == []

    t4 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z4 = KVS.reader(t4, :args)
    r = :kvs.save(t4)
    log("next t4:", t4)

    assert length(z4) == p
    log(:end, "test bidirectional (new)")
  end

  test "partial take forward full" do
    log(:st, "partial take forward full")
    id = {:partial, :kvs.seq([], [])}
    x = 7
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    KVS.reader(id: rid) = :kvs.save(:kvs.reader(id))
    p = 3
    IO.inspect :kvs.all id

    t1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z1 = KVS.reader(t1, :args)
    :kvs.save(t1)
    log("next t1:", t1)

    t2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z2 = KVS.reader(t2, :args)
    :kvs.save(t2)
    log("next t2:", t2)

    t3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z3 = KVS.reader(t3, :args)
    :kvs.save(t3)
    log("next t3:", t3)

    assert length(z3) == 1
    assert :lists.reverse(z1) ++ :lists.reverse(z2) ++ z3 == :kvs.all(id)
    log(:end, "partial take forward full")
  end

  test "take with empy" do
    log(:st, "take with empy")
    id = {:partial, :kvs.seq([], [])}
    x = 6
    p = 3
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", :kvs.seq([],[]), [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    IO.inspect :kvs.all(id)
    rid = KVS.reader(r, :id)
    t1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z1 = KVS.reader(t1, :args)
    r = :kvs.save(t1)
    log("next t1:", t1)

    t2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z2 = KVS.reader(t2, :args)
    r = :kvs.save(t2)
    log("next t2:", t2)

    t3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z3 = KVS.reader(t3, :args)
    r = :kvs.save(t3)
    log("next t3:", t3)
    assert  z3 == []

    KVS.reader(id: tid) = :kvs.save(KVS.reader(t3, dir: 1, pos: 0))
    n1 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p))
    nz1 = KVS.reader(n1, :args)
    :kvs.save n1
    log("prev b1:", n1)

    n2 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p))
    nz2 = KVS.reader(n2, :args)
    :kvs.save n2
    log("prev b2:", n2)

    assert z2 ++ z1 == nz1 ++ nz2
    log(:end, "take with empy")
  end

  test "test prev" do
    log(:st, "test prev")
    id = {:partial, :kvs.seq([], [])}
    x = 6
    p = 3
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", :kvs.seq([],[]), [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    IO.inspect :kvs.all(id)

    t1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z1 = KVS.reader(t1, :args)
    r = :kvs.save(t1)
    log("next z1:", z1)

    t2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    z2 = KVS.reader(t2, :args)

    KVS.reader(id: tid) = :kvs.save(KVS.reader(t2, dir: 1, pos: 0))
    log("next z2:", z2)

    n1 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p))
    nz1 = tl(:lists.reverse(KVS.reader(n1, :args)))
    :kvs.save(n1)
    log("prev nz1:", nz1)

    n2 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p))
    nz2 = KVS.reader(n2, :args)
    :kvs.save n2
    log("prev n2:", n2)

    assert length(nz2) == p
    assert nz2 == z1

    n3 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p))
    nz3 = KVS.reader(n3, :args)
    :kvs.save(KVS.reader(n3, dir: 0))
    log("prev nz3:", nz3)

    assert nz3 = []

    log(:end, "test prev")

  end

  def log(x,cursor) do
     IO.inspect {x,cursor}
  end
end

