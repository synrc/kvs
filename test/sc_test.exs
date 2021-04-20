ExUnit.start()

defmodule Sc.Test do
    use ExUnit.Case, async: false
    require KVS
    import Record
    @moduledoc """
        refined old scenarios
    """

    defrecord(:msg, id: [], body: [])

    setup do: (on_exit(fn -> :ok = :kvs.leave();:ok = :kvs.destroy() end);:kvs.join())
    setup kvs, do: [
        id0: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/duck") end, :lists.seq(1,10)),
        id1: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/luck") end, :lists.seq(1,10)),
        id2: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/truck") end, :lists.seq(1,10)),
        id3: :lists.map(fn _ -> :kvs.save(:kvs.add(KVS.writer(:kvs.writer(:sym),
                                        args: msg(id: :kvs.seq([],[]))))) end, :lists.seq(1,10))]
    test "basic", kvs do
        id1 = "/crm/luck"
        id2 = "/crm/truck"
        :kvs.save(:kvs.writer(id1))
        :kvs.save(:kvs.writer(id2))

        KVS.reader(id: rid1) = :kvs.save(:kvs.reader(id1))
        KVS.reader(id: rid2) = :kvs.save(:kvs.reader(id2))
        x1 = :kvs.take(KVS.reader(:kvs.load_reader(rid1), args: 20))
        x2 = :kvs.take(KVS.reader(:kvs.load_reader(rid2), args: 20))
        b = :kvs.feed(id1)
        assert 10 == length(b)
        assert :kvs.all("/crm/truck") == KVS.reader(x2, :args)
        assert KVS.reader(x1, :args) == b
        assert length(KVS.reader(x1, :args)) == length(KVS.reader(x2, :args))
    end

    test "sym",kvs do
        :kvs.save(:kvs.writer(:sym))
        KVS.writer(args: last) = Enum.at(kvs[:id3],-1)
        {:ok, KVS.writer(id: :sym, count: 10, cache: last)} = :kvs.get(:writer, :sym)
    end

    test "take back full" do
        feed = "/crm/duck"
        :kvs.save(:kvs.writer(feed))
        KVS.reader(id: rid) = :kvs.save(:kvs.reader(feed))
        t = KVS.reader(args: a1) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 10))
        assert a1 == :kvs.feed(feed)
        :kvs.save(KVS.reader(t, dir: 1))
        KVS.reader(args: a2) = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: 10))
        assert :lists.reverse(a2) == :kvs.feed(feed)
  end

    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'
end
