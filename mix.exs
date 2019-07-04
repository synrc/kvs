defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [app: :kvs, version: "6.7.4", description: "Abstract Chain Database", package: package(), deps: deps()]
  end

  def application do
    [mod: {:kvs, []}, applications: [:rocksdb]]
  end

  defp package do
    [
      files: ~w(include man etc lib src LICENSE mix.exs README.md rebar.config sys.config),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end

  defp deps do
    [{:ex_doc, ">= 0.0.0", only: :dev}]
  end
end
