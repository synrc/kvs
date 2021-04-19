ExUnit.start()

defmodule Fd.Test do
    use ExUnit.Case, async: true
    require KVS
    import Record

    defrecord(:msg, id: [], body: [])

    setup do: (on_exit(fn -> :ok = :kvs.leave();:ok = :kvs.destroy() end);:kvs.join())
    setup kvs, do: [
        id0: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/duck") end, :lists.seq(1,10)),
        id1: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/luck") end, :lists.seq(1,10)),
        id2: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/truck") end, :lists.seq(1,10))]

    test "reader", kvs do
        ltop = Enum.at(kvs[:id1],0)
        dtop = Enum.at(kvs[:id0],0)
        ttop = Enum.at(kvs[:id2],0)
        assert KVS.reader(feed: "/crm/luck", count: 10, dir: 0, args: [], cache: {:msg, ^ltop, "/crm/luck"}) = :kvs.reader("/crm/luck")
        assert KVS.reader(feed: "/crm/duck", count: 10, dir: 0, args: [], cache: {:msg, ^dtop, "/crm/duck"}) = :kvs.reader("/crm/duck")
        assert KVS.reader(feed: "/crm/truck", count: 10, dir: 0, args: [], cache: {:msg, ^ttop, "/crm/truck"}) = :kvs.reader("/crm/truck")
        assert KVS.reader(feed: "/crm", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "/crm/duck"}) = :kvs.reader("/crm")
        assert KVS.reader(feed: "/noroute", count: 0, dir: 0, args: []) = :kvs.reader("/noroute")
        assert KVS.reader(feed: "/", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "/crm/duck"}) = :kvs.reader("/")
        assert KVS.reader(feed: "", count: 0, dir: 0, args: [], cache: []) = :kvs.reader([])
    end

    test "range", kvs do
        ltop = Enum.at(kvs[:id1],0)
        dtop = Enum.at(kvs[:id0],0)
        lbot = Enum.at(kvs[:id1],9)

        assert KVS.reader(feed: "/crm/luck", count: 10, dir: 0, args: [], cache: {:msg, ^ltop, "/crm/luck"}) = :kvs.top(:kvs.reader("/crm/luck"))
        assert KVS.reader(feed: "/crm", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "/crm/duck"}) = :kvs.top(:kvs.reader("/crm"))
        assert KVS.reader(feed: "/", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "/crm/duck"}) = :kvs.top(:kvs.reader("/"))

        assert KVS.reader(feed: "/crm/luck", count: 10, dir: 0, args: [], cache: {:msg, ^lbot, "/crm/luck"}) = :kvs.bot(:kvs.reader("/crm/luck"))
        assert KVS.reader(feed: "/crm", count: 0, dir: 0, args: [], cache: []) = :kvs.bot(:kvs.reader("/crm"))
        assert KVS.reader(feed: "/", count: 0, dir: 0, args: [], cache: []) = :kvs.bot(:kvs.reader("/"))
    end

    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'
end
