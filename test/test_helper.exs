ExUnit.start()

defmodule :rocksdb do
  def destroy(_, _), do: :ok
  def iterator(_, _), do: {:ok, nil}
  def iterator_close(_), do: :ok
  def iterator_move(_, _), do: {:error, :not_found}
  def open(_, _), do: {:ok, nil}
  def put(_, _, _, _), do: :ok
  def delete(_, _, _), do: :ok
  def delete_range(_, _, _, _), do: :ok
  def compact_range(_, _, _, _), do: :ok
  def close(_), do: :ok
  def get(_, _, _), do: :not_found
  def get_property(_, _), do: {:ok, "0"}
end
