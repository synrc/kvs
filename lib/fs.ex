defmodule :kvs_fs do
  require Record
  require KVS

  def db, do: ""
  def start, do: :ok
  def stop, do: :ok
  def destroy, do: :ok
  def destroy(_), do: :ok
  def match(_), do: []
  def index_match(_, _), do: []
  def version, do: {:version, "KVS FS"}
  def leave, do: :ok
  def leave(_), do: :ok

  def dir do
    for f <- :filelib.wildcard(~c"data/*"), :filelib.is_dir(f) do
      {:table, f}
    end
  end

  def join(_node, _) do
    :filelib.ensure_dir(to_charlist(dir_name() <> "/"))
    initialize()
  end

  def initialize() do
    :mnesia.create_schema([node()])
    for m <- :kvs.modules(), do: :kvs.initialize(:kvs_fs, m)
    :mnesia.wait_for_tables(for(t <- :kvs.tables(), do: KVS.table(t, :name)), :infinity)
  end

  def index(_tab, _key, _value), do: []

  def get(table_name, key, _) do
    hash_key = hashkey(key)
    {:ok, root} = dir(table_name)
    file = Path.join(root, to_string(hash_key))

    case File.read(file) do
      {:ok, binary} -> {:ok, :erlang.binary_to_term(binary, [:safe])}
      {:error, reason} -> {:error, reason}
    end
  end

  def put(r), do: put(r, db())

  def put(records, x) when is_list(records) do
    Enum.map(records, &put(&1, x))
  end

  def put(record, _) do
    table_name = elem(record, 0)
    hash_key = hashkey(elem(record, 1))
    binary_value = :erlang.term_to_binary(record)

    {:ok, root} = dir(table_name)
    file = Path.join(root, to_string(hash_key))
    :filelib.ensure_dir(to_charlist(file))
    File.write!(file, binary_value, [:raw, :binary, :sync])
  end

  def dir_name, do: "data"

  def dir(table_name) do
    {:ok, cwd} = :file.get_cwd()
    table_path = Path.join(dir_name(), to_string(table_name))

    case :filelib.safe_relative_path(to_charlist(table_path), cwd) do
      :unsafe -> {:error, {:unsafe, table_path}}
      target -> {:ok, to_string(target)}
    end
  end

  def hashkey(key) do
    key
    |> :erlang.term_to_binary()
    |> :crypto.hash(:sha)
    |> :base64.encode()
    |> encode()
  end

  def delete(table_name, key, _) do
    case get(table_name, key, []) do
      {:ok, _} ->
        {:ok, root} = dir(table_name)
        hash_key = hashkey(key)
        file = Path.join(root, to_string(hash_key))
        File.rm!(file)
        :ok

      {:error, x} ->
        {:error, x}
    end
  end

  def delete_range(_, _, _), do: {:error, :not_found}
  def keys(_, _), do: []
  def key_match(_, _, _), do: []

  def count(record_name) do
    dir = Path.join(dir_name(), to_string(record_name))
    length(:filelib.fold_files(to_charlist(dir), ~c"", true, fn a, acc -> [a | acc] end, []))
  end

  def all(r, _) do
    dir = Path.join(dir_name(), to_string(r))
    files = :filelib.fold_files(to_charlist(dir), ~c"", true, fn a, acc -> [a | acc] end, [])

    List.flatten(
      for f <- files do
        case File.read(f) do
          {:ok, binary} -> :erlang.binary_to_term(binary, [:safe])
          {:error, _} -> []
        end
      end
    )
  end

  def seq(record_name, incr), do: :kvs_mnesia.seq(record_name, incr)

  def create_table(name, _options) do
    {:ok, root} = dir(name)
    File.mkdir_p!(root)
  end

  def add_table_index(_record, _field), do: :ok

  def dump, do: :ok

  # URL ENCODE

  def encode(b) when is_binary(b), do: encode(:erlang.binary_to_list(b))
  def encode([c | cs]) when c >= ?a and c <= ?z, do: [c | encode(cs)]
  def encode([c | cs]) when c >= ?A and c <= ?Z, do: [c | encode(cs)]
  def encode([c | cs]) when c >= ?0 and c <= ?9, do: [c | encode(cs)]
  def encode([c | cs]) when c == 0x20, do: [?+ | encode(cs)]
  def encode([c | cs]) when c in [?-, ?_, ?., ?!, ?~, ?*, ?', ?(, ?)], do: [c | encode(cs)]
  def encode([c | cs]) when c <= 0x7F, do: escape_byte(c) ++ encode(cs)

  import Bitwise

  def encode([c | cs]) when c >= 0x7F and c <= 0x07FF do
    escape_byte((c >>> 6) + 0xC0) ++
      escape_byte((c &&& 0x3F) + 0x80) ++
      encode(cs)
  end

  def encode([c | cs]) when c > 0x07FF do
    escape_byte((c >>> 12) + 0xE0) ++
      escape_byte((0x3F &&& (c >>> 6)) + 0x80) ++
      escape_byte((c &&& 0x3F) + 0x80) ++
      encode(cs)
  end

  def encode([c | cs]), do: escape_byte(c) ++ encode(cs)
  def encode([]), do: []


  def hex_octet(n) when n <= 9, do: [?0 + n]
  def hex_octet(n) when n > 15, do: hex_octet(n >>> 4) ++ hex_octet(n &&& 15)
  def hex_octet(n), do: [n - 10 + ?a]

  def escape_byte(c), do: normalize(hex_octet(c))

  def normalize(h) when length(h) == 1, do: ~c"%0" ++ h
  def normalize(h), do: ~c"%" ++ h
end
