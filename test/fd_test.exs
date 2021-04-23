ExUnit.start()

defmodule Fd.Test do
    use ExUnit.Case, async: false
    require KVS
    import Record

    test "fd_test", kvs do
       assert "//erp" = :kvs_rocks.fd "//erp/orgs"
       assert "/erp" = :kvs_rocks.fd "/erp/orgs"
    end

    test "key_test", kvs do
       assert "//erp/orgs" = :kvs_rocks.key "/erp/orgs"
       assert "/erp/orgs"  = :kvs_rocks.key "erp/orgs"
    end

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
        assert KVS.reader(feed: "//crm/luck", count: 10, dir: 0, args: [], cache: {:msg, ^ltop, "//crm/luck"}) = :kvs.reader("/crm/luck")
        assert KVS.reader(feed: "//crm/duck", count: 10, dir: 0, args: [], cache: {:msg, ^dtop, "//crm/duck"}) = :kvs.reader("/crm/duck")
        assert KVS.reader(feed: "//crm/truck", count: 10, dir: 0, args: [], cache: {:msg, ^ttop, "//crm/truck"}) = :kvs.reader("/crm/truck")
        assert KVS.reader(feed: "//crm", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "//crm/duck"}) = :kvs.reader("/crm")
        assert KVS.reader(feed: "//noroute", count: 0, dir: 0, args: []) = :kvs.reader("/noroute")
        assert KVS.reader(feed: "//", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "//crm/duck"}) = :kvs.reader("/")
        assert KVS.reader(count: 0, dir: 0, args: [], cache: []) = :kvs.reader([])
    end

    test "range", kvs do
        ltop = Enum.at(kvs[:id1],0)
        dtop = Enum.at(kvs[:id0],0)
        lbot = Enum.at(kvs[:id1],9)

        assert KVS.reader(feed: "//crm/luck", count: 10, dir: 0, args: [], cache: {:msg, ^ltop, "//crm/luck"}) = :kvs.top(:kvs.reader("/crm/luck"))
        assert KVS.reader(feed: "//crm", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "//crm/duck"}) = :kvs.top(:kvs.reader("/crm"))
        assert KVS.reader(feed: "//", count: 0, dir: 0, args: [], cache: {:msg, ^dtop, "//crm/duck"}) = :kvs.top(:kvs.reader("/"))

        assert KVS.reader(feed: "//crm/luck", count: 10, dir: 0, args: [], cache: {:msg, ^lbot, "//crm/luck"}) = :kvs.bot(:kvs.reader("/crm/luck"))
        assert KVS.reader(feed: "//crm", count: 0, dir: 0, args: [], cache: []) = :kvs.bot(:kvs.reader("/crm"))
        assert KVS.reader(feed: "//", count: 0, dir: 0, args: [], cache: []) = :kvs.bot(:kvs.reader("/"))
    end

    test "next", kvs do
        last = msg(id: Enum.at(kvs[:id1],9))
        r0 = KVS.reader(id: rid) = :kvs.save(:kvs.top(:kvs.reader("/crm/luck")))
        kvs[:id1] |> Enum.with_index 
                  |> Enum.each(fn {id,9} ->
                r = :kvs.load_reader(rid)
                r01 = :kvs.next(r)
                assert r1 = KVS.reader(feed: "//crm/luck", cache: c1, count: 10, dir: 0, args: [^last]) = r01
                assert KVS.reader(args: [], feed: "//crm/luck", cache: c1) = :kvs.save(r1)
            {id,i} -> 
                v = msg(id: Enum.at(kvs[:id1],i))
                c = Enum.at(kvs[:id1],i+1)
                r = :kvs.load_reader(rid)
                assert r1 = KVS.reader(feed: "//crm/luck", cache: {:msg,^c,"//crm/luck"}, count: 10, dir: 0, args: [^v]) = :kvs.next(r)
                assert KVS.reader(args: [], feed: "//crm/luck", cache: {:msg,^c,"//crm/luck"}) = :kvs.save(r1)
            end)
        r = :kvs.load_reader(rid)

        assert r == :kvs.next(r)
        assert r == KVS.reader(:kvs.next(:kvs.bot(r)), args: [])
    end

    test "prev", kvs do
        out = Enum.at(kvs[:id0],9)
        KVS.reader(id: rid) = :kvs.save(:kvs.bot(:kvs.reader("/crm/luck")))
        ids = kvs[:id1] |> Enum.reverse
        ids |> Enum.with_index
            |> Enum.each(fn {id,9} ->
                r = :kvs.load_reader(rid)
                v = msg(id: Enum.at(ids, 9))
                assert r1 = KVS.reader(feed: "//crm/luck", cache: {:msg, ^out, "//crm/duck"}, count: 10, args: [^v]) = :kvs.prev(r)
                assert KVS.reader(args: [], feed: "//crm/luck", cache: c1) = :kvs.save(r1)
            {id,i} ->
                r = :kvs.load_reader(rid)
                v = msg(id: Enum.at(ids, i))
                c = Enum.at(ids, i+1)
                assert r1 = KVS.reader(feed: "//crm/luck", cache: {:msg, ^c, "//crm/luck"}, count: 10, args: [^v]) = :kvs.prev(r)
                assert KVS.reader(args: [], feed: "//crm/luck", cache: {:msg, ^c, "//crm/luck"}) = :kvs.save(r1)
            end)
        r = :kvs.load_reader(rid)
        assert r = :kvs.prev(r)
        assert r = KVS.reader(:kvs.prev(:kvs.top(r)), args: [])                 
    end


    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'
end
