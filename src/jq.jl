__precompile__(true)
module jq

using PyCall
using Dates
import Pandas
import DataFrames
import TimeSeries
import TimeSeries.TimeArray

include("timeseries.jl")


export JQ, finTable, shareHolder, get_Price,getBS,getCF,getIS

const JQ = PyNULL()

function __init__()
    copy!(JQ, pyimport("jqdatasdk"))
    JQ.auth("15202875236","1qaz2wsx")
    print(JQ.get_query_count())
    #JQ[:auth]("15202875236","1qaz2wsx")
    #jq[:get_query_count]()
end



#####################################################
#%% 财务数据：利润表
function finTable(code::String,year::Int,tables::Symbol=:STK_INCOME_STATEMENT)
    q1 = JQ[:finance][tables]
    f1 = JQ[:finance][tables][:code]==code
    f2 = JQ[:finance][tables][:end_date]==Date(year,12,31)
    f3 = JQ[:finance][tables][:report_type] ==0 #本期报表
    qq = JQ[:query](q1)[:filter](f1,f2,f3)
    dfq = JQ[:finance][:run_query](qq)
    df2 = py2df(dfq,Index=false)
    return df2
end

getCF(code,year) = finTable(code,year,:STK_CASHFLOW_STATEMENT)
getIS(code,year) = finTable(code,year, :STK_INCOME_STATEMENT)
getBS(code,year) = finTable(code,year, :STK_BALANCE_SHEET)

#%% 十大股东，查询获得的是当日或者是当日后最近的一个日期的公布数据
function shareHolder(code::String; end_date::Date=today()-Month(6),rank::Int=1)
    tables =:STK_SHAREHOLDER_TOP10
    q1 = JQ[:finance][tables]
    f1 = JQ[:finance][tables][:code]==code
    f2 = JQ[:finance][tables][:end_date]>=end_date #大于还是小于？取决于如何选取数据
    f3 = JQ[:finance][tables][:shareholder_rank] == rank #股东名次
    qq = JQ[:query](q1)[:filter](f1,f2,f3)[:limit](1)
    dfq = JQ[:finance][:run_query](qq)
    df2 = py2df(dfq)
    return df2
end

#%% 价格数据
function get_price(code::String,start_date::Date=today();end_date::Date=start_date,fields::Array=["close"], skip_paused::Bool=false,freq::String="daily",fq::String="pre")
    #获取价格数据
    p1 = JQ[:get_price](code, start_date=start_date, end_date=end_date, fields=fields, skip_paused=skip_paused, frequency=freq, fq=fq)
    p1 = Pandas.DataFrame(p1)
    p1 = Pandas.reset_index(p1)
    df = DataFrames.DataFrame(p1)
    return df
end

#%% 股票编码
function normalize_code(code)
    jqcode = JQ[:normalize_code](code)
    return jqcode
end

export normalize_code



end # module
