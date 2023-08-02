defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "10.8.2",
      description: "KVS Abstract Chain Database",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [mod: {:kvs, []}, applications: [:mnesia,:ex_doc]]
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
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:rocksdb, "~> 1.6.0", only: :test}
    ]
  end
end
