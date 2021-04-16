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

    #: real cache {:feed, :msg, id}
    test "top",  kvs, do: (r0=:kvs.reader(:feed); assert KVS.reader(r0, cache: {:msg, Enum.at(kvs[:ids],0)}, dir: 0) == :kvs.top(r0))
    test "bot",  kvs, do: (r0=:kvs.reader(:feed); assert KVS.reader(r0, cache: {:msg, Enum.at(kvs[:ids],9)}, dir: 1) == :kvs.bot(r0))
    

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

    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'

end
