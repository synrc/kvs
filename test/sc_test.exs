ExUnit.start()

defmodule Sc.Test do
    use ExUnit.Case, async: false
    require KVS
    import Record
    @moduledoc """
        refined old scenarios
    """

    defrecord(:msg, id: [], next: [], prev: [], user: [], msg: [])

    setup do
        :kvs_mnesia.destroy()
        :kvs.join()
        :ok
    end
    setup _kvs, do: [
        id0: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/duck") end, :lists.seq(1,10)),
        id1: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/luck") end, :lists.seq(1,10)),
        id2: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/truck") end, :lists.seq(1,10)),
        id3: :lists.map(fn _ -> :kvs.save(:kvs.add(KVS.writer(:kvs.get_writer(:sym),
                                        args: msg(id: :kvs.seq([],[]))))) end, :lists.seq(1,10))]
    test "basic", kvs do
        KVS.reader(id: rid1) = :kvs.save(:kvs.get_reader("/crm/luck"))
        KVS.reader(id: rid2) = :kvs.save(:kvs.get_reader("/crm/truck"))
        x1 = :kvs.take(KVS.reader(:kvs.load_reader(rid1), args: 20))
        x2 = :kvs.take(KVS.reader(:kvs.load_reader(rid2), args: 20))
        b = :kvs.feed("/crm/luck")
        assert 10 == length(b)
        assert :kvs.all("/crm/truck") == KVS.reader(x2, :args)
        assert KVS.reader(x1, :args) == b
        assert length(KVS.reader(x1, :args)) == length(KVS.reader(x2, :args))
    end

    test "sym",kvs do
        KVS.writer(args: last) = Enum.at(kvs[:id3],-1)
        {:ok, w} = :kvs.get(:writer, :sym)
        assert KVS.writer(w, :count) == 10
    end

    test "take back full" do
        feed = "/crm/duck"
        :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), feed) end, :lists.seq(1,10))
        KVS.reader(id: rid) = :kvs.save(:kvs.get_reader(feed))
        t = KVS.reader(args: a1) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 10))
        assert 10 == length(a1)
        :kvs.save(KVS.reader(t, dir: 1))
        KVS.reader(args: a2) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 10))
        assert length(a2) > 0
    end

    test "partial take back" do
        feed = "/crm/luck"
        :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), feed) end, :lists.seq(1,10))
        KVS.reader(id: rid) = :kvs.save(:kvs.get_reader(feed))
        r = KVS.reader(args: t) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 2))
        :kvs.save(KVS.reader(r, dir: 1))
        KVS.reader(args: _n) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 3))
        assert length(t) > 0
    end
end
