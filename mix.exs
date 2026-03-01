defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "13.2.28",
      description: "KVS Abstract Chain Database",
      package: package(),
      deps: deps()
    ]
  end

  def application do
     [ mod: {:kvs, []}, extra_applications: [ :mnesia ] ]
  end

  defp package do
    [
      files: ~w(include man config lib LICENSE mix.exs README.md),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev}
#     {:rocksdb, "~> 2.2.0", only: :test}
    ]
  end
end
