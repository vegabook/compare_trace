defmodule Recon do

  @bqdirs ["~/data/trace_data/bondcliq/parsed", 
           "~/data/trace_data/bondcliq/downloaded"]
  @madirs ["~/data/trace_data/marketaxess/"]

  @bqno ["20240501_20240603", "FINRA_RAW"]
  @bqrawno ["20240501_20240603"]
  @mano ["matreasury"]

  @bqyes [".csv"]
  @bqrawyes [".csv", "FINRA_RAW"]
  @mayes [".csv"]



  def files(dirs, yesyes, nono) do
    dirs
    |> Enum.map(fn x -> Path.expand(x) end)
    |> Enum.map(fn x -> Enum.map(File.ls!(x), fn y -> Path.join(x, y) end) end)
    |> List.flatten
    |> Enum.filter(fn x ->  Enum.all?(Enum.map(yesyes, fn y -> String.contains?(x, y) end)) end)
    |> Enum.reject(fn x ->  Enum.any?(Enum.map(nono, fn y -> String.contains?(x, y) end)) end)
  end

  def bqfiles() do
    files(@bqdirs, @bqyes, @bqno)
  end

  def bqrawfiles() do
    files(@bqdirs, @bqrawyes, @bqrawno)
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

  def bq_fields(df) do
    cusip = df["55"] 
      |> Explorer.Series.to_list
      |> Enum.map(fn x -> if x == nil, do: "nil", else: x end)
      |> Enum.map(fn x -> if String.length(x) == 9, do: x, else: x end)
    msn = df["278"] |> Explorer.Series.to_list
    tdate = df["272"] 
      |> Explorer.Series.to_list
      |> Enum.map(fn x -> Timex.parse!(x, "{D} {Mshort} {YYYY}") |> Timex.to_date end) 
    ttime = df["273"] 
      |> Explorer.Series.to_list
      |> Enum.map(fn x -> Time.from_iso8601!(x) end)
    jdt = Enum.map(Enum.zip(tdate, ttime), fn x -> NaiveDateTime.new!(elem(x, 0), elem(x, 1)) 
                                                    |> Timex.to_datetime
                                                    |> DateTime.to_unix end)
    #Enum.zip([cusip, msn, jdt])
    Enum.zip([cusip])
  end

  def bqraw_fields(df) do
    cusip = df["CUSIP"]
      |> Explorer.Series.to_list
    msn = df["MSN"]
      |> Explorer.Series.to_list
    jdt = df["Execution_DateTime"]
      |> Explorer.Series.to_list
      |> Enum.map(&Integer.to_string(&1))
      |> Enum.map(fn x -> Timex.parse!(x, "{ASN1:GeneralizedTime}") 
                                                    |> Timex.to_datetime
                                                    |> DateTime.to_unix end)
    #Enum.zip([cusip, msn, jdt])
    Enum.zip([cusip])

  end


end
