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

    defp log(x), do: IO.puts '#{inspect(x)}'
    defp log(m, x), do: IO.puts '#{m} #{inspect(x)}'

end
