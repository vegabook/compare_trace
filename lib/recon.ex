defmodule Recon do

  @bqdirs ["~/data/trace_data/bondcliq/parsed", 
           "~/data/trace_data/bondcliq/downloaded"]
  @madirs ["~/data/trace_data/marketaxess/"]

  @bqno1 ["20240501_20240603", "FINRA_RAW"]
  @bqno2 ["20240501_20240603"]
  @mano ["matreasury"]

  @bqyes1 [".csv"]
  @bqyes2 [".csv", "FINRA_RAW"]
  @mayes [".csv"]



  def files(dirs, yesyes, nono) do
    dirs
    |> Enum.map(fn x -> Path.expand(x) end)
    |> Enum.map(fn x -> Enum.map(File.ls!(x), fn y -> Path.join(x, y) end) end)
    |> List.flatten
    |> Enum.filter(fn x ->  Enum.all?(Enum.map(yesyes, fn y -> String.contains?(x, y) end)) end)
    |> Enum.reject(fn x ->  Enum.any?(Enum.map(nono, fn y -> String.contains?(x, y) end)) end)

  end

  def bqfiles1() do
    files(@bqdirs, @bqyes1, @bqno1)
  end

  def bqfiles2() do
    files(@bqdirs, @bqyes2, @bqno2)
  end

  def mafiles() do
    files(@madirs, @mayes, @mano)
  end

  def filedates(files) do
    # regex 8 consecutive digits
    Enum.map(files, fn x -> Regex.run(~r/\d{8}/, x) |> Enum.at(0) end)
  end

  def match_files(files1, files2) do
    dates1 = filedates(files1)
    dates2 = filedates(files2)
    interdates = MapSet.intersection(MapSet.new(dates1), MapSet.new(dates2)) 
      |> MapSet.to_list()
    f1 = Enum.map(interdates, fn x -> Enum.filter(files1, fn y -> String.contains?(y, x) end) |> Enum.at(0) end)
    f2 = Enum.map(interdates, fn x -> Enum.filter(files2, fn y -> String.contains?(y, x) end) |> Enum.at(0) end)
    Enum.zip(f1, f2)
  end 


  def read_csv(file) do
    #File.stream!(file, [read_ahead: 1000], 1000)
    #|> CSV.decode()
    Explorer.DataFrame.from_csv!(file, [nil_values: ["null"], infer_schema_length: 10000000])
  end




end
