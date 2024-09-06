defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "11.9.1",
      description: "KVS Abstract Chain Database",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [mod: {:kvs, []}, extra_applications: [:mnesia,:ex_doc]]
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
      {:ex_doc, ">= 0.0.0", only: :dev}
#     {:rocksdb, "~> 1.8.0", only: :test}
    ]
  end
end
