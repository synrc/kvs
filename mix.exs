defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "13.4.14",
      description: "KVS Key-Value Store Abstraction Layer",
      package: package(),
      deps: deps(),
      xref: [exclude: [:rocksdb]]
    ]
  end

  def application do
    [mod: {:kvs, []}, extra_applications: [:mnesia]]
  end

  defp package do
    [
      files: ~w(include man config lib LICENSE mix.exs README.md),
      licenses: ["MIT"],
      maintainers: ["Namdak Tonpa"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end

  defp deps do
    deps = [
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]

    if System.get_env("KVS_BACKEND") == "rocksdb" do
      deps ++ [{:rocksdb, "~> 2.5.0", optional: true}]
    else
      deps
    end
  end
end
