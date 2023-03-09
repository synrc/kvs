ExUnit.start()

defmodule St.Test do
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

    test "take-ø", kvs do
        r = KVS.reader() = :kvs.reader("/empty-feed")
        assert r1 = KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 1))
        assert r1 = KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 1, dir: 1))
        assert r2 = KVS.reader(feed: "//empty-feed", args: []) = :kvs.next(r)
        assert r3 = KVS.reader(feed: "//empty-feed", args: []) = :kvs.prev(r)
        assert r1 = KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 100))
        assert r1 = KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(r, args: 100, dir: 1))
        KVS.reader(id: rid) = :kvs.save(r1)
        assert rs1 = KVS.reader(id: rid) = :kvs.load_reader(rid)
        assert KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 5))
        assert KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 5, dir: 1))
        assert KVS.reader(feed: "//empty-feed", args: []) = :kvs.next(rs1)
        assert KVS.reader(feed: "//empty-feed", args: []) = :kvs.prev(rs1)
        assert KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 0))
        assert KVS.reader(feed: "//empty-feed", args: []) = :kvs.take(KVS.reader(rs1, args: 0, dir: 1))
    end

    test "take-0", kvs do
        feed = "//crm/personal/Реєстратор А1/in/doc"
        assert r = KVS.reader(id: rid, args: []) = :kvs.reader("/crm/personal/Реєстратор А1/in/doc")

        assert KVS.reader(id: ^rid, feed: ^feed, args: []) = :kvs.take(KVS.reader(r, args: 0, dir: 0))
        assert KVS.reader(id: ^rid, feed: ^feed, args: []) = :kvs.take(KVS.reader(r, args: -1, dir: 0))

        assert r1 = KVS.reader(id: ^rid, feed: ^feed, args: a01) = :kvs.take(KVS.reader(r,  args: 10, dir: 0))
        assert kvs[:id2] |> Enum.map(&msg(id: &1)) == a01
        assert KVS.reader(id: ^rid, feed: ^feed, args: []) = :kvs.take(KVS.reader(r1, args: 10, dir: 0))

        assert KVS.reader(id: ^rid, feed: ^feed, args: af) = :kvs.take(KVS.reader(r, args: 100, dir: 0))
        assert kvs[:id2] |> Enum.map(&msg(id: &1)) == af

        assert r2 = KVS.reader(id: ^rid, feed: ^feed, args: a03) = :kvs.take(KVS.reader(r,  args: 3, dir: 0))
        assert Enum.take(kvs[:id2],3) |> Enum.map(&msg(id: &1)) == a03

        assert KVS.reader(id: ^rid, feed: ^feed, args: a07) = :kvs.take(KVS.reader(r2, args: 7, dir: 0))
        assert Enum.drop(kvs[:id2],3) |> Enum.map(&msg(id: &1)) == a07

        assert KVS.reader(id: ^rid, feed: ^feed, args: a17) = :kvs.take(KVS.reader(r2, args: 100, dir: 0))
        assert Enum.drop(kvs[:id2],3) |> Enum.map(&msg(id: &1)) == a17
    end

    test "take-1", kvs do
        feed = "//crm/personal/Реєстратор А1/in/mail"
        top = Enum.at(kvs[:id1],0)
        bot = Enum.at(kvs[:id1],9)
        tpm = Enum.take(kvs[:id1],1) |> Enum.map(&msg(id: &1))

        assert r = KVS.reader(id: rid, args: [], cache: {:msg, ^top, ^feed}, count: 10) = :kvs.reader("/crm/personal/Реєстратор А1/in/mail")
        assert r = :kvs.top(r)
        assert KVS.reader(id: ^rid, feed: ^feed, args: ^tpm, dir: 1) = :kvs.take(KVS.reader(r, args: 1, dir: 1))
        assert KVS.reader(id: ^rid, feed: ^feed, args: ^tpm, dir: 1) = :kvs.take(KVS.reader(r, args: 100, dir: 1))

        assert r1 = KVS.reader(feed: ^feed, count: 10, args: [], cache: {:msg,^bot,^feed}) = :kvs.bot(r)

        assert r2 = KVS.reader(feed: ^feed, count: 10, args: a01) = :kvs.take(KVS.reader(r1, args: 5, dir: 1))
        x01 = Enum.drop(kvs[:id1],5) |> Enum.map(&msg(id: &1)) |> Enum.reverse
        assert x01 == a01

        assert r3 = KVS.reader(feed: ^feed, count: 10, args: a02) = :kvs.take(KVS.reader(r2, args: 10, dir: 1))
        x02 = Enum.take(kvs[:id1],5) |> Enum.map(&msg(id: &1)) |> Enum.reverse
        assert x02 == a02

        assert KVS.reader(feed: ^feed, count: 10, args: []) = :kvs.take(KVS.reader(r3, args: 20, dir: 1))
    end

    test "drop", kvs do
        feed = "/feed"
        assert r = KVS.reader(id: rid, args: []) = :kvs.save(:kvs.reader(:feed))
        assert r1 = KVS.reader(id: ^rid, feed: ^feed, args: []) = :kvs.drop(KVS.reader(r,  args: 10, dir: 0))

        kvs[:ids] |> Enum.map(&msg(id: &1)) 
                  |> Enum.each(&assert(
                    KVS.reader(id: rid, feed: ^feed, args: [], cache: &1) =
                        :kvs.save(:kvs.drop(KVS.reader(:kvs.load_reader(rid), args: 1, dir: 0)))))

        assert r2 = KVS.reader(id: ^rid, feed: ^feed, args: [], cache: c1) = :kvs.drop(KVS.reader(r, args: 1, dir: 0))
        assert {:msg, Enum.at(kvs[:ids], 1), feed} == c1

        assert KVS.reader(id: ^rid, feed: ^feed, args: [], cache: c2) = :kvs.drop(KVS.reader(r2, args: 5, dir: 0))
        assert {:msg, Enum.at(kvs[:ids], 6), feed} == c2
        assert KVS.reader(id: ^rid, feed: ^feed, args: []) = :kvs.drop(KVS.reader(r1, args: 100))
    end

    test "feed",kvs do
        docs  = :kvs.all("/crm/personal/Реєстратор А1/in/doc")
        ducks = :kvs.all("/crm/personal/Реєстратор А1/in/directory/duck")
        mail  = :kvs.all("/crm/personal/Реєстратор А1/in/mail")
        total = :kvs.all("/crm/personal/Реєстратор А1/in")

        assert ducks++docs++mail == total

        assert docs  = :kvs.feed("/crm/personal/Реєстратор А1/in/doc")
        assert ducks = :kvs.feed("/crm/personal/Реєстратор А1/in/directory/duck")
        assert mail  = :kvs.feed("/crm/personal/Реєстратор А1/in/mail")
        assert total = :kvs.feed("/crm/personal/Реєстратор А1/in")

        assert :kvs.feed("/crm/personal/Реєстратор А1/in/directory/duck") 
            ++ :kvs.feed("/crm/personal/Реєстратор А1/in/doc")
            ++ :kvs.feed("/crm/personal/Реєстратор А1/in/mail") == :kvs.feed("/crm/personal/Реєстратор А1/in")
    end

    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'

end
