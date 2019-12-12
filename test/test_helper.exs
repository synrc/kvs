require KVS
:kvs.join()
ExUnit.start()

defmodule BPE.Test do
  use ExUnit.Case, async: true

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
    b = :kvs.feed(id1)

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
    r = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r, :id)), args: 20))
    b = :kvs.feed(id)
    # mnesia
    assert KVS.reader(t, :args) == b
  end

  test "take back" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r, :id)), args: 5))
    KVS.reader(id: tid) = :kvs.save(KVS.reader(t, dir: 1))
    n = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: 5, dir: 1))
    b = :kvs.feed(id)
    assert KVS.reader(t, :args) == b
    assert KVS.reader(n, :args) == b
  end

  test "take back2" do
    id = {:partial, :kvs.seq([], [])}
    x = 3
    p = 2
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r, :id)), args: p))
    KVS.reader(id: tid) = :kvs.save(KVS.reader(t, dir: 1))
    n = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p, dir: 1))
    assert KVS.reader(t, :args) == KVS.reader(n, :args)
  end

  test "take back3" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    p =2
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    t1 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    
    z1 = :lists.reverse(KVS.reader(t1, :args))
    r = :kvs.save(t1)

    t2 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z2 = :lists.reverse(KVS.reader(t2, :args))
    r = :kvs.save(t2)

    t3 = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p, dir: 0))
    z3 = :lists.reverse(KVS.reader(t3, :args))

    KVS.reader(id: tid) = :kvs.save(KVS.reader(t3, dir: 1))

    n1 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p, dir: 1))
    nz1 = :lists.reverse(KVS.reader(n1, :args))
    KVS.reader(id: tid) = :kvs.save(KVS.reader(n1, dir: 1))
   
    n2 = :kvs.take(KVS.reader(:kvs.load_reader(tid), args: p, dir: 1))
    nz2 = :lists.reverse(KVS.reader(n2, :args))
    r = :kvs.save(KVS.reader(n2, dir: 1))
   # assert :lists.reverse(z1 ++ z2 ++ z3) == :lists.reverse(nz2 ++ nz1 ++ z3) 
  end


  test "partial take" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    p = 2

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    z1 = a
    r = :kvs.save(t)
    IO.inspect({cache, r, a})
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    assert length(a) == p

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    z2 = a
    IO.inspect({cache, r, a})
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    assert length(a) == p

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    z3 = a
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    IO.inspect({cache, t, a})
    assert length(a) == 1

    assert :lists.reverse(z1 ++ z2 ++ z3) == :kvs.feed(id)
  end
end
