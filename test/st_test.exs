ExUnit.start()

defmodule St.Test do
    use ExUnit.Case, async: false
    import Record
    require KVS

    defrecord(:msg, id: [], next: [], prev: [], user: [], msg: [])

    setup do
        on_exit(fn -> :ok = :kvs.leave();:ok = :kvs_rocks.destroy() end)
        :kvs.join()
        :ok
    end
    setup _kvs do
        :kvs_mnesia.destroy()
        :kvs.join()
        [
            ids: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), :feed) end, :lists.seq(1,10)),
            id0: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/personal/Реєстратор А1/in/directory/duck") end, :lists.seq(1,10)),
            id1: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/personal/Реєстратор А1/in/mail") end, :lists.seq(1,10)),
            id2: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/personal/Реєстратор А1/in/doc") end, :lists.seq(1,10))
        ]
    end

    test "take-ø", kvs do
        r = KVS.reader() = :kvs.get_reader("/empty-feed")
        r1 = :kvs.take(KVS.reader(r, args: 1))
        assert Record.is_record(r1, :reader)
        r1 = :kvs.take(KVS.reader(r, args: 1, dir: 1))
        assert Record.is_record(r1, :reader)
        r2 = :kvs.next(r)
        assert {:error, :empty} == r2
        r3 = :kvs.prev(r)
        assert {:error, :empty} == r3
        r1 = :kvs.take(KVS.reader(r, args: 100))
        assert Record.is_record(r1, :reader)
        r1 = :kvs.take(KVS.reader(r, args: 100, dir: 1))
        assert Record.is_record(r1, :reader)
        KVS.reader(id: rid) = :kvs.save(r1)
        rs1 = :kvs.load_reader(rid)
        assert Record.is_record(rs1, :reader)
        res = :kvs.take(KVS.reader(rs1, args: 5))
        assert Record.is_record(res, :reader)
        res = :kvs.take(KVS.reader(rs1, args: 5, dir: 1))
        assert Record.is_record(res, :reader)
        res = :kvs.next(rs1)
        assert {:error, :empty} == res
        res = :kvs.prev(rs1)
        assert {:error, :empty} == res
        res = :kvs.take(KVS.reader(rs1, args: 0))
        assert Record.is_record(res, :reader)
        res = :kvs.take(KVS.reader(rs1, args: 0, dir: 1))
        assert Record.is_record(res, :reader)
    end

    test "take-0", kvs do
        feed = "/crm/personal/Реєстратор А1/in/doc"
        r = :kvs.get_reader("/crm/personal/Реєстратор А1/in/doc")
        assert Record.is_record(r, :reader)

        res = :kvs.take(KVS.reader(r, args: 0, dir: 0))
        assert Record.is_record(res, :reader)
        res = :kvs.take(KVS.reader(r, args: -1, dir: 0))
        assert Record.is_record(res, :reader)

        r1 = :kvs.take(KVS.reader(r,  args: 10, dir: 0))
        assert Record.is_record(r1, :reader)
        assert kvs[:id2] == KVS.reader(r1, :args) |> Enum.map(&elem(&1, 1))
        res = :kvs.take(KVS.reader(r1, args: 10, dir: 0))
        assert Record.is_record(res, :reader)

        res = :kvs.take(KVS.reader(r, args: 100, dir: 0))
        assert Record.is_record(res, :reader)
        assert kvs[:id2] == KVS.reader(res, :args) |> Enum.map(&elem(&1, 1))

        r2 = :kvs.take(KVS.reader(r,  args: 3, dir: 0))
        assert Record.is_record(r2, :reader)
        assert Enum.take(kvs[:id2],3) == KVS.reader(r2, :args) |> Enum.map(&elem(&1, 1))

        res = :kvs.take(KVS.reader(r2, args: 7, dir: 0))
        assert Record.is_record(res, :reader)
        assert length(KVS.reader(res, :args)) == 7

        res = :kvs.take(KVS.reader(r2, args: 100, dir: 0))
        assert Record.is_record(res, :reader)
    end

    test "take-1", kvs do
        feed = "/crm/personal/Реєстратор А1/in/mail"
        top = Enum.at(kvs[:id1],0)
        bot = Enum.at(kvs[:id1],9)
        tpm = Enum.take(kvs[:id1],1) |> Enum.map(&msg(id: &1))

        r = :kvs.get_reader("/crm/personal/Реєстратор А1/in/mail")
        assert Record.is_record(r, :reader)
        assert KVS.reader(r, :id) == KVS.reader(:kvs.top(r), :id)
        res = :kvs.take(KVS.reader(r, args: 1, dir: 1))
        assert Record.is_record(res, :reader)
        res = :kvs.take(KVS.reader(r, args: 100, dir: 1))
        assert Record.is_record(res, :reader)

        r1 = :kvs.top(r)
        assert Record.is_record(r1, :reader)

        r2 = :kvs.take(KVS.reader(r1, args: 5, dir: 1))
        assert Record.is_record(r2, :reader)
        x01 = Enum.drop(kvs[:id1],5) |> Enum.reverse
        assert x01 == KVS.reader(r2, :args) |> Enum.map(&elem(&1, 1))

        r3 = :kvs.take(KVS.reader(r2, args: 10, dir: 1))
        assert Record.is_record(r3, :reader)
        x02 = Enum.take(kvs[:id1],5) |> Enum.reverse
        assert x02 == KVS.reader(r3, :args) |> Enum.map(&elem(&1, 1))

        res = :kvs.take(KVS.reader(r3, args: 20, dir: 1))
        assert Record.is_record(res, :reader)
    end

    test "drop", kvs do
        feed = :feed
        r = :kvs.save(:kvs.get_reader(:feed))
        assert Record.is_record(r, :reader)
        r1 = :kvs.drop(KVS.reader(r,  args: 10, dir: 0))
        assert Record.is_record(r1, :reader)

        kvs[:ids] |> Enum.each(fn _ ->
            KVS.reader(id: rid_tmp, feed: ^feed) = :kvs.save(:kvs.drop(KVS.reader(:kvs.load_reader(KVS.reader(r, :id)), args: 1, dir: 0)))
        end)

        r2 = :kvs.drop(KVS.reader(r, args: 1, dir: 0))
        assert Record.is_record(r2, :reader)
        assert Enum.at(kvs[:ids], 1) == elem(KVS.reader(r2, :cache), 1)

        res = :kvs.drop(KVS.reader(r2, args: 5, dir: 0))
        assert Record.is_record(res, :reader)
        assert Enum.at(kvs[:ids], 6) == elem(KVS.reader(res, :cache), 1)
        res = :kvs.drop(KVS.reader(r1, args: 100))
        assert Record.is_record(res, :reader)
    end

    # test "feed",kvs do
    #     docs  = :kvs.all("/crm/personal/Реєстратор А1/in/doc")
    #     ducks = :kvs.all("/crm/personal/Реєстратор А1/in/directory/duck")
    #     mail  = :kvs.all("/crm/personal/Реєстратор А1/in/mail")
    #     total = :kvs.all("/crm/personal/Реєстратор А1/in")
    #
    #     assert ducks++docs++mail == total
    #
    #     assert :kvs.feed("/crm/personal/Реєстратор А1/in/directory/duck")
    #         ++ :kvs.feed("/crm/personal/Реєстратор А1/in/doc")
    #         ++ :kvs.feed("/crm/personal/Реєстратор А1/in/mail") == :kvs.feed("/crm/personal/Реєстратор А1/in")
    # end
end
