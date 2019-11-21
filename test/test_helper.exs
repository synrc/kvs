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
        []
    end

    assert :lists.reverse(KVS.reader(x1, :args)) == :lists.reverse(b)
    assert length(KVS.reader(x1, :args)) == length(KVS.reader(x2, :args))
    assert x == length(b)
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

  test "partial take" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    p = 2
    IO.inspect(:kvs.feed(id))

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    IO.inspect({cache, r, a})
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    assert length(a) == p

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    IO.inspect({cache, r, a})
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    assert length(a) == p

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    IO.inspect({cache, t, a})
    assert length(a) == 1
  end
end
