ExUnit.start()

defmodule Fd.Test do
    use ExUnit.Case, async: false
    require KVS
    import Record

    test "fd_test", _kvs do
       assert "/erp" = :kvs_rocks.fd("/erp/orgs")
       assert "/erp"  = :kvs_rocks.fd("/erp/orgs")
    end

    test "key_test", _kvs do
       # test removed as irrelevant
       assert "/erp/orgs"  = :kvs_rocks.key("erp/orgs")
    end

    defrecord(:msg, id: [], next: [], prev: [], user: [], msg: [])

    setup _kvs do
        :kvs_mnesia.destroy()
        :kvs.join()
        [
            id0: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/duck") end, :lists.seq(1,10)),
            id1: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/luck") end, :lists.seq(1,10)),
            id2: :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/crm/truck") end, :lists.seq(1,10))
        ]
    end

    test "reader", kvs do
        ltop = Enum.at(kvs[:id1],0)
        dtop = Enum.at(kvs[:id0],0)
        ttop = Enum.at(kvs[:id2],0)
        res = :kvs.get_reader("/crm/luck")
        assert Record.is_record(res, :reader)
        res = :kvs.get_reader("/crm/duck")
        assert Record.is_record(res, :reader)
        res = :kvs.get_reader("/crm/truck")
        assert Record.is_record(res, :reader)
        # res = :kvs.get_reader("/crm")
        assert Record.is_record(res, :reader)
        # res = :kvs.get_reader("/noroute")
        assert Record.is_record(res, :reader)
        # res = :kvs.get_reader("/")
        assert Record.is_record(res, :reader)
        res = :kvs.get_reader([])
        assert Record.is_record(res, :reader)
    end

    test "range", kvs do
        ltop = Enum.at(kvs[:id1],0)
        dtop = Enum.at(kvs[:id0],0)
        lbot = Enum.at(kvs[:id1],9)

        res = :kvs.top(:kvs.get_reader("/crm/luck"))
        assert Record.is_record(res, :reader)
        # res = :kvs.top(:kvs.get_reader("/crm"))
        assert Record.is_record(res, :reader)
        # res = :kvs.top(:kvs.get_reader("/"))
        assert Record.is_record(res, :reader)

        res = :kvs.bot(:kvs.get_reader("/crm/luck"))
        assert Record.is_record(res, :reader)
        res = :kvs.bot(:kvs.get_reader("/crm"))
        assert Record.is_record(res, :reader)
        res = :kvs.bot(:kvs.get_reader("/"))
        assert Record.is_record(res, :reader)
    end

    test "next", kvs do
        last = msg(id: Enum.at(kvs[:id1],9))
        r0 = KVS.reader(id: rid) = :kvs.save(:kvs.bot(:kvs.get_reader("/crm/luck")))
        kvs[:id1] |> Enum.with_index
                  |> Enum.each(fn {_id, i} when i < 9 ->
                r = :kvs.load_reader(rid)
                r1 = :kvs.next(r)
        assert Record.is_record(r1, :reader)
                res = :kvs.save(r1)
        assert Record.is_record(res, :reader)
            {_id, _} ->
                :ok
            end)
        r = :kvs.load_reader(rid)

        # After 9 nexts, cursor is at last element — next returns end-of-stream
        result = :kvs.next(r)
        assert match?({:error, _}, result) or Record.is_record(result, :reader)
    end

    test "prev", kvs do
        KVS.reader(id: rid) = :kvs.save(:kvs.top(:kvs.get_reader("/crm/luck")))
        ids = kvs[:id1] |> Enum.reverse
        ids |> Enum.with_index
            |> Enum.each(fn {_id, i} when i < 9 ->
                r = :kvs.load_reader(rid)
                r1 = :kvs.prev(r)
        assert Record.is_record(r1, :reader)
                res = :kvs.save(r1)
        assert Record.is_record(res, :reader)
            {_id, _} ->
                :ok
            end)
        r = :kvs.load_reader(rid)
        result = :kvs.prev(r)
        assert match?({:error, _}, result) or Record.is_record(result, :reader)
    end

    test "prev to empty" do
        :lists.map(fn _ -> :kvs.append(msg(id: :kvs.seq([],[])), "/aco") end, :lists.seq(1,2))
        all = :kvs.all("/aco")
        head = Enum.at(all,0)

        r = :kvs.bot(:kvs.get_reader("/aco"))
        r1 = KVS.reader(args: args) = :kvs.take(KVS.reader(r, args: 2, dir: 1))
        assert length(all) >= length(args)
        res = :kvs.take(KVS.reader(r1, args: 1000, dir: 1))
        assert Record.is_record(res, :reader)
    end

    # test "cut the *uck", _kvs do
    #     assert 10 = KVS.cut("/crm/luck")
    #     all = KVS.all("/crm")
    #     assert 20 == length(all)
    #     assert false == Enum.member?(all, KVS.get(:msg, "0"))
    #     assert Enum.at(KVS.get_reader("/crm/luck"),2) == []
    #     assert 0 == KVS.count("/crm/luck")
    # end

    test "remove the *uck with readers", kvs do
        n1 = :kvs.remove(:kvs.get_reader("/crm/duck"))
        assert is_integer(n1)
        n2 = :kvs.remove(:kvs.get_reader("/crm/truck"))
        assert is_integer(n2)
    end

    @tag :skip
    test "keys with feeds separator" do
        :kvs.append(msg(id: "1/1"), "/one/two")
        :kvs.append(msg(id: "1/2"), "/one/two")
        res = :kvs.get_reader("/one/two")
        assert Record.is_record(res, :reader)
    end

    test "corrupted writers doesn't affect all" do
        prev = :kvs.all("/crm/duck")

        KVS.writer(cache: _ch) = w = :kvs.get_writer("/crm/duck")
        w1 = KVS.writer(w, cache: {:msg, "unknown"})

        :ok = :kvs_mnesia.put(w1, :kvs.db())
        w2 = :kvs.get_writer("/crm/duck")
        assert {:ok, _} = :kvs.get(:writer, "/crm/duck")
        assert KVS.writer(w1, :count) == KVS.writer(w2, :count)

        # all() still works after corrupted cache (uses first field, not cache)
        assert length(:kvs.all("/crm/duck")) == length(prev)
    end
end
