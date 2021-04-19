ExUnit.start()

defmodule ST.Test do
    use ExUnit.Case, async: false
    import Record
    require KVS

    defrecord(:msg, id: [], body: [])
    setup do: (on_exit(fn -> :ok = :kvs.leave();:ok = :kvs_rocks.destroy() end);:kvs.join())
    setup kvs, do: [
        ids: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), :feed) end, :lists.seq(1,10)),
        id0: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/personal/Реєстратор А1/in/directory/duck") end, :lists.seq(1,10)),
        id1: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/personal/Реєстратор А1/in/mail") end, :lists.seq(1,10)),
        id2: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/personal/Реєстратор А1/in/doc") end, :lists.seq(1,10))]

    test "al0", kvs, do: assert kvs[:ids] |> Enum.map(&msg(id: &1)) == :kvs.all(:feed)
    test "al1", kvs, do: assert (kvs[:id0] ++ kvs[:id2] ++ kvs[:id1]) |> Enum.map(&msg(id: &1)) == :kvs.all("/crm/personal/Реєстратор А1/in")
    
    #: old behaviour is reversed ? 
    test "fe0", kvs, do: assert kvs[:ids] |> Enum.reverse |> Enum.map(&msg(id: &1)) == :kvs.feed(:feed)

    #: real cache {:feed, :msg, id}
    test "top",  kvs, do: (r0=:kvs.reader(:feed); assert KVS.reader(r0, cache: {:msg, Enum.at(kvs[:ids],0), "/feed"}, dir: 0) == :kvs.top(r0))
    test "bot",  kvs, do: (r0=:kvs.reader(:feed); assert KVS.reader(r0, cache: {:msg, Enum.at(kvs[:ids],9), "/feed"}, dir: 1) == :kvs.bot(r0))
    
    test "next", kvs do
        KVS.reader(id: rid) = :kvs.save(:kvs.top(:kvs.reader(:feed)))
        kvs[:ids] |> Enum.each(&assert(KVS.reader(cache: {:msg,&1,"/feed"}) = :kvs.next(:kvs.load_reader(rid))))
    end

    test "prev", kvs do
        KVS.reader(id: rid) = :kvs.save(:kvs.bot(:kvs.reader(:feed)))
        kvs[:ids] |> Enum.reverse |> Enum.each(&assert KVS.reader(cache: {:msg,&1,"/feed"}) = :kvs.prev(:kvs.load_reader(rid)))
    end
   

    test "take-ø", kvs do
        r = KVS.reader() = :kvs.reader("/empty-feed")
        assert r1 = KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 1))
        assert r1 = KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 1, dir: 1))
        assert r2 = KVS.reader(feed: "/empty-feed", args: []) = :kvs.next(r)
        assert r3 = KVS.reader(feed: "/empty-feed", args: []) = :kvs.prev(r)
        assert r1 = KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 100))
        assert r1 = KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 100, dir: 1))
        KVS.reader(id: rid) = :kvs.save(r1)
        assert rs1 = KVS.reader(id: rid) = :kvs.load_reader(rid)
        assert KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 5))
        assert KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 5, dir: 1))
        assert KVS.reader(feed: "/empty-feed", args: []) = :kvs.next(rs1)
        assert KVS.reader(feed: "/empty-feed", args: []) = :kvs.prev(rs1)
        assert KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 0))
        assert KVS.reader(feed: "/empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 0, dir: 1))
    end

    test "drop", kvs do
        assert r = KVS.reader(id: rid, args: [], cache: c0) = :kvs.save(:kvs.reader(:feed))
        assert r1 = KVS.reader(id: ^rid, feed: :feed, args: []) = :kvs.drop(KVS.reader(r,  args: 10, dir: 0))

        kvs[:ids] |> Enum.map(&msg(id: &1)) 
                  |> Enum.each(&assert(KVS.reader(id: rid, feed: :feed, args: [], cache: &1) = :kvs.save(:kvs.drop(KVS.reader(:kvs.load_reader(rid), args: 1, dir: 0)))))

        assert r2 = KVS.reader(id: ^rid, feed: :feed, args: [], cache: c1) = :kvs.drop(KVS.reader(r, args: 1, dir: 0))
        assert {:msg, Enum.at(kvs[:ids], 1)} == c1
        assert r3 = KVS.reader(id: ^rid, feed: :feed, args: [], cache: c2) = :kvs.drop(KVS.reader(r2, args: 5, dir: 0))
        assert {:msg, Enum.at(kvs[:ids], 6)} == c2
        assert r4 = KVS.reader(id: ^rid, feed: :feed, args: [], cache: c3) = :kvs.drop(KVS.reader(r1, args: 100))
        assert {:msg, Enum.at(kvs[:ids],9)} == c3
    end

    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'

end
