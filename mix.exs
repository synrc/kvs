defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "7.9.1",
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
      files: ~w(include man config test lib src LICENSE mix.exs README.md rebar.config sys.config),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.11", only: :dev},
      {:rocksdb, "~> 1.3.2", only: :test}
    ]
  end
end
