defmodule Recon do

  @bqdirs ["~/data/trace_data/bondcliq/parsed", 
           "~/data/trace_data/bondcliq/downloaded"]
  @madirs ["~/data/trace_data/marketaxess/"]

  @bqno ["20240501_20240603", "20240506", "FINRA_RAW"]
  @bqrawno ["20240501_20240603"]
  @mano ["matreasury"]

  @bqyes [".csv"]
  @bqrawyes [".csv", "FINRA_RAW"]
  @mayes [".csv"]
  @months %{
    "Jan" => 1,
    "Feb" => 2,
    "Mar" => 3,
    "Apr" => 4,
    "May" => 5,
    "Jun" => 6,
    "Jul" => 7,
    "Aug" => 8,
    "Sep" => 9,
    "Oct" => 10,
    "Nov" => 11,
    "Dec" => 12
  }


  alias Explorer.DataFrame, as: DF
  alias Explorer.Series, as: SE
  require Explorer.Query

  
  # associate bondcliq fields with marketaxess fields
  def bq_ma_assoc do
    %{"CUSIP" => "55", 
      "MSN" => "278",
      "Price" => "270"}
  end 
 
  # associate raw bondcliq fields with bondcliq fields
  # TODO
  def bqraw_bq_assoc do
    %{"CUSIP" => "55", 
      "MSN" =>  "278",
      "Price" => "270"}
  end


  def files(dirs, yesyes, nono) do
    dirs
    |> Enum.map(fn x -> Path.expand(x) end)
    |> Enum.map(fn x -> Enum.map(File.ls!(x), fn y -> Path.join(x, y) end) end)
    |> List.flatten
    |> Enum.filter(fn x ->  Enum.all?(Enum.map(yesyes, fn y -> String.contains?(x, y) end)) end)
    |> Enum.reject(fn x ->  Enum.any?(Enum.map(nono, fn y -> String.contains?(x, y) end)) end)
  end

  def bq_files() do
    files(@bqdirs, @bqyes, @bqno)
  end

  def bqraw_files() do
    files(@bqdirs, @bqrawyes, @bqrawno)
  end

  def ma_files() do
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
    DF.from_csv!(file, [nil_values: ["null"], infer_schema_length: 10000000])
  end

  def bq_fields(df) do
    cusip = df["55"] 
      |> SE.to_list
      |> Enum.map(fn x -> if x == nil, do: "nil", else: x end)
      |> Enum.map(fn x -> if String.length(x) == 9, do: x, else: x end)
    msn = df["278"] |> SE.to_list

    dateparse = fn x -> 
      [d, m, y] = String.split(x, " ")
      {id, im, iy} = {String.to_integer(d), Map.get(@months, m), String.to_integer(y)}
      Date.new!(iy, im, id)
    end

    tdate = df["272"] 
      |> SE.to_list
      |> Enum.map(fn x -> dateparse.(x) end)
    ttime = df["273"] 
      |> SE.to_list
      |> Enum.map(fn x -> Time.from_iso8601!(x) end)
    jdt = Enum.map(Enum.zip(tdate, ttime), fn x -> NaiveDateTime.new!(elem(x, 0), elem(x, 1)) 
                                                    |> Timex.to_datetime
                                                    |> DateTime.to_unix end)
    Enum.zip([cusip, msn, jdt])
    Enum.zip([cusip])
  end

  def bqraw_fields(df) do
    cusip = df["CUSIP"]
      |> SE.to_list
    msn = df["MSN"]
      |> SE.to_list
    jdt = df["Execution_DateTime"]
      |> SE.to_list
      |> Enum.map(&Integer.to_string(&1))
      |> Enum.map(fn x -> Timex.parse!(x, "{ASN1:GeneralizedTime}") 
                                                    |> Timex.to_datetime
                                                    |> DateTime.to_unix end)
    Enum.zip([cusip, msn, jdt])
    Enum.zip([cusip])
  end


  def ma_fields(df) do
    cusip = df["CUSIP"]
      |> SE.to_list
    msn = df["SEQUENCENUMBER"]
      |> SE.to_list
    tdate = df["EFFECTIVEDATE"]
      |> SE.to_list
      |> Enum.map(fn x -> if x == nil, do: "nil", else: x end)
      |> Enum.map(fn x -> Timex.parse!(x, "{M}/{D}/{YYYY}") |> Timex.to_date end) 
    ttime = df["EFFECTIVETIME"]
      |> SE.to_list
      |> Enum.map(fn x -> if x == nil, do: "nil", else: x end)
      |> Enum.map(fn x -> Time.from_iso8601!(x) end)
    jdt = Enum.map(Enum.zip(tdate, ttime), fn x -> NaiveDateTime.new!(elem(x, 0), elem(x, 1)) 
                                                    |> Timex.to_datetime
                                                    |> DateTime.to_unix end)
    Enum.zip([cusip, msn, jdt])
    Enum.zip([cusip])
  end


  def bq_ma(do_columns \\ false) do
    fntups = match_files(bq_files(), ma_files()) 
    fntups_nopath = Enum.map(fntups, fn x -> {Path.basename(elem(x, 0)), Path.basename(elem(x, 1))} end)
    dftups = fntups
    |> Enum.map(fn x -> {read_csv(elem(x, 0)), read_csv(elem(x, 1))} end)
    lens = dftups
    |> Enum.map(fn x -> {DF.n_rows(elem(x, 0)), DF.n_rows(elem(x, 1))} end)
    dfnn = dftups
    |> Enum.map(fn x -> {DF.drop_nil(elem(x, 0), ["55", "278", "272", "273"]), DF.drop_nil(elem(x, 1), ["CUSIP", "SEQUENCENUMBER", "EFFECTIVEDATE", "EFFECTIVETIME"])} end)
    fldtups = dfnn
    |>  Enum.map(fn x -> {bq_fields(elem(x, 0)), ma_fields(elem(x, 1))} end)
    inters = fldtups
    |> Enum.map(fn x -> 
        m1 = MapSet.new(elem(x, 0))
        m2 = MapSet.new(elem(x, 1))
        mi = MapSet.intersection(m1, m2)
        mdm1m2 = MapSet.difference(m1, m2)
        mdm2m1 = MapSet.difference(m2, m1)
        {m1, m2, mi, mdm1m2, mdm2m1} end)
    interlens = inters
    |> Enum.map(fn x ->
        m1 = elem(x, 0)
        m2 = elem(x, 1)
        mi = elem(x, 2)
        mdm1m2 = elem(x, 3)
        mdm2m1 = elem(x, 4)
        [m1len: MapSet.to_list(m1) |> length, 
         m2len: MapSet.to_list(m2) |> length, 
         milen: MapSet.to_list(mi) |> length, 
         mdm1m2: MapSet.to_list(mdm1m2) |> length,
         mdm2m1: MapSet.to_list(mdm2m1) |> length]
      end)

    if do_columns do
 
      is_nil = fn s -> s |> Explorer.Series.not_equal(nil) end
      is_empty = fn s -> s |> Explorer.Series.not_equal("") end

      allcols = Enum.map(dftups, fn x -> 
        # combinations of columns
        for i <- DF.names(elem(x, 0)) do 
          for j <- DF.names(elem(x, 1)) do
            IO.puts("i: #{i}, j: #{j}")
            {i, j, SE.in(elem(x, 0)[i] |> SE.to_list |> Enum.filter(fn x -> x != nil end) |> Enum.filter(fn x -> x != "" end) |> SE.from_list |> SE.cast(:string),
                         elem(x, 1)[j] |> SE.to_list |> Enum.filter(fn x -> x != nil end) |> Enum.filter(fn x -> x != "" end) |> SE.from_list |> SE.cast(:string))
              |> SE.cast(:integer) 
              |> SE.sum()}
          end
        end
        |> List.flatten
        |> Enum.filter(fn x -> elem(x, 2) != 0 end)
      end)
      Enum.zip([fntups, allcols])
    else
      Enum.zip([fntups_nopath, lens, interlens])
    end
  end
    
      
  def bqraw_bq(do_columns \\ false) do
    fntups = match_files(bqraw_files(), bq_files()) 
    fntups_nopath = Enum.map(fntups, fn x -> {Path.basename(elem(x, 0)), Path.basename(elem(x, 1))} end)
    dftups = fntups
    |> Enum.map(fn x -> {read_csv(elem(x, 0)), read_csv(elem(x, 1))} end)
    lens = dftups
    |> Enum.map(fn x -> {DF.n_rows(elem(x, 0)), DF.n_rows(elem(x, 1))} end)
    dfnn = dftups
    |> Enum.map(fn x -> {DF.drop_nil(elem(x, 0), ["CUSIP", "MSN", "Execution_DateTime"]), DF.drop_nil(elem(x ,1), ["55", "278", "272", "273"])} end)
    fldtups = dfnn
    |>  Enum.map(fn x -> {bqraw_fields(elem(x, 0)), bq_fields(elem(x, 1)) } end)
    inters = fldtups
    |> Enum.map(fn x -> 
        m1 = MapSet.new(elem(x, 0))
        m2 = MapSet.new(elem(x, 1))
        mi = MapSet.intersection(m1, m2)
        mdm1m2 = MapSet.difference(m1, m2)
        mdm2m1 = MapSet.difference(m2, m1)
        {m1, m2, mi, mdm1m2, mdm2m1} end)
    interlens = inters
    |> Enum.map(fn x -> 
        m1 = MapSet.new(elem(x, 0))
        m2 = MapSet.new(elem(x, 1))
        mi = MapSet.intersection(m1, m2)
        mdm1m2 = MapSet.difference(m1, m2)
        mdm2m1 = MapSet.difference(m2, m1)
        {m1, m2, mi, mdm1m2, mdm2m1} end)
    interlens = inters
    |> Enum.map(fn x ->
        m1 = elem(x, 0)
        m2 = elem(x, 1)
        mi = elem(x, 2)
        mdm1m2 = elem(x, 3)
        mdm2m1 = elem(x, 4)
        [m1len: MapSet.to_list(m1) |> length, 
         m2len: MapSet.to_list(m2) |> length, 
         milen: MapSet.to_list(mi) |> length, 
         mdm1m2: MapSet.to_list(mdm1m2) |> length,
         mdm2m1: MapSet.to_list(mdm2m1) |> length]
      end)

    if do_columns do
 
      is_nil = fn s -> s |> Explorer.Series.not_equal(nil) end
      is_empty = fn s -> s |> Explorer.Series.not_equal("") end


      allcols = Enum.map(dftups, fn x -> 
        # combinations of columns
        for i <- DF.names(elem(x, 0)) do 
          for j <- DF.names(elem(x, 1)) do
            IO.puts("i: #{i}, j: #{j}")
            {i, j, SE.in(elem(x, 0)[i] |> SE.to_list |> Enum.filter(fn x -> x != nil end) |> Enum.filter(fn x -> x != "" end) |> SE.from_list |> SE.cast(:string),
                         elem(x, 1)[j] |> SE.to_list |> Enum.filter(fn x -> x != nil end) |> Enum.filter(fn x -> x != "" end) |> SE.from_list |> SE.cast(:string))
              |> SE.cast(:integer) 
              |> SE.sum()}
          end
        end
        |> List.flatten
        |> Enum.filter(fn x -> elem(x, 2) != 0 end)
      end)
      Enum.zip([fntups, allcols])
    else
      Enum.zip([fntups_nopath, lens, interlens])
    end
  end



end
