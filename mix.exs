defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "9.9.0",
      description: "KVS Abstract Chain Database",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [mod: {:kvs, []}]
  end

  defp package do
    [
      files: ~w(include man config lib src LICENSE mix.exs README.md),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.19", only: :dev},
      {:rocksdb, "~> 1.6.0", only: :test}
    ]
  end
end
