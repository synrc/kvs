defmodule KVS.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvs,
      version: "11.10.0",
      description: "KVS Abstract Chain Database",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [mod: {:kvs, []}, extra_applications: [:mnesia]]
  end

  defp package do
    [
      files: ~w(config lib mix.exs README.md LICENSE),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/synrc/kvs"}
    ]
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end
end
