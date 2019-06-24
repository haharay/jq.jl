

export py2df, TimeArray, ta2df,month_diff


## 把下载jq数据框转化为DataFrames.jl
function py2df(pydf; index::Bool=false)
    pandasDF = Pandas.DataFrame(pydf)
    if index
        pandasDF = Pandas.reset_index(pandasDF)
    end
    df = DataFrames.DataFrame(pandasDF)
    return df
end

function ta2df(ta::TimeArray; colnames=Symbol[], timestamp=:time)
    if length(colnames) != 0
        #colnames_str = [string(s) for s in colnames]
        ta = ta[colnames...]
    end
    #分别定义，以保留类型
    df1 = DataFrames.DataFrame(TimeSeries.timestamp(ta))
    df2 = DataFrames.DataFrame(values(ta))
    df = hcat(df1,df2,makeunique=true)
    colns = vcat(timestamp, TimeSeries.colnames(ta))
    names!(df,colns)
    df[timestamp] = Dates.Date.(df[timestamp])
    df
end

function TimeArray(df::DataFrames.DataFrame; colnames=Symbol[], timestamp=:time)
    if length(colnames) != 0
        df = df[vcat(timestamp, colnames)]
        colnames = Symbol.(colnames)
    else
        colnames = names(df)
        colnames = filter(s->s!=timestamp, colnames)
    end
    if !(issorted(df,timestamp))
        df = sort(df,timestamp,rev=true)
    end
    a_timestamp = Dates.Date.(df[timestamp])
    a_values = Matrix(df[colnames])
    ta = TimeArray(a_timestamp, a_values, colnames)
    ta
end

function month_diff(ta::TimeSeries.TimeArray;n=1)
    x1 = diff(ta,n)
    tmin = minimum(month.(timestamp(x1)))
    x1 = x1[month.(timestamp(x1)) .!= tmin]
    x11 = when(ta, month, tmin)
    x = [x1;x11]
    return x
end


########RRRRRRRRRRRRRRRRRRRRRRRRRRRR################
