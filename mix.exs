defmodule CompareTrace.MixProject do
  use Mix.Project

  def project do
    [
      app: :compare_trace,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      applications: applications(Mix.env)
    ]
  end


  defp applications(:dev), do: applications(:all) ++ [:remix]
  defp applications(_all), do: [:logger]


  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:csv, "~> 3.2"},
      {:explorer, "~> 0.8.2"},
      {:remix, "~> 0.0.1", only: :dev}
    ]
  end
end
