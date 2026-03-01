defmodule KVS.Mixfile do
  use Mix.Project
  def project do
    [ app: :kvs,
      version: "11.10.0",
      description: "KVS Key-Value Abstraction Layer",
      package: package(),
      deps: deps()
    ]
  end
  def application do
    [ mod: {:kvs, []},
      extra_applications: [ :mnesia, :crypto, :rocksdb ]
    ]
  end
  def package do
    [ files: ~w(config lib mix.exs README.md LICENSE),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end
  def deps do
    [ {:ex_doc, ">= 0.0.0", only: :dev},
      {:rocksdb, "~> 1.9.0"}
    ]
  end
end
