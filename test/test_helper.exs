
:kvs.join
ExUnit.start()

defmodule BPE.Test do
  use ExUnit.Case, async: true
  test "kvs" do
    assert :kvs.check == :ok
  end
end
