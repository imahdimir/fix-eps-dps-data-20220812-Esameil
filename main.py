##

"""

    """

##

from pathlib import Path
import datetime
from functools import partial

import pandas as pd
from mirutil.funcs import norm_fa_str as norm
from mirutil.funcs import read_data_according_to_type as rdata
from mirutil.funcs import save_as_prq_wo_index as sprq
from mirutil.funcs import save_df_as_a_nice_xl as sxl
from mirutil.funcs import persian_tools_jdate_from_iso_format_jdate_str as isojd
from mirutil.funcs import persian_tools_jdate_from_int_format_jdate as intjd


def main() :


  pass

  ## read data & keep related cols


  df = rdata('export.xlsx')

  cols = {
      "symbol"   : None ,
      "shrout"   : None ,
      "eps"      : None ,
      "dps"      : None ,
      "earning"  : None ,
      "dividend" : None ,
      "date"     : None ,
      "date_eps" : None ,
      "date_fin" : None ,
      }

  df = df[cols.keys()]

  ## make date_fin col as persian tools jdate type
  dtf = 'date_fin'
  df[dtf] = df[dtf].apply(intjd)

  ## normalize symbols
  sym = 'symbol'
  df[sym] = df[sym].apply(norm)

  ## keep unique duos of symbol and date_fin
  df = df.drop_duplicates(subset = [sym , dtf])

  ## read fiscal year date data
  df1 = rdata('fis.xlsx')

  ## make the date col as persian tools jdate type
  fis = 'FiscalYearJDate'
  df1[fis] = df1[fis].apply(isojd)

  ## make yr col in both datasets
  df1['yr'] = df1[fis].apply(lambda x : str(x.year))
  df['yr'] = df[dtf].apply(lambda x : str(x.year) if pd.notna(x) else None)

  ## add data from fiscal year dataset to 0 dataset by merging on [sym , yr]
  btic = 'BaseTicker'
  df1 = df1[[btic , fis , 'yr']]

  df = df.merge(df1 ,
                left_on = [sym , 'yr'] ,
                right_on = [btic , 'yr'] ,
                how = 'left')

  df = df.drop(columns = btic)

  ## compare fiscal year data from 2 datasets
  df['sfy'] = df[dtf].eq(df[fis])

  msk = df[fis].notna() & df[dtf].notna() & df['sfy'].eq(False)
  len(df[msk])

  msk = df[fis].isna() | df[dtf].isna()
  df.loc[msk , 'sfy'] = None

  ## read outstanding shares daily data
  df1 = rdata('outs.prq')

  tic = 'Ticker'
  outs = 'OutstandingShares'
  jdt = 'JDate'

  df1[jdt] = df1[jdt].apply(isojd)

  df1 = df1.sort_values(jdt)

  ##
  # def _find_nearest_date_and_shares_to_each_gp(_gpo , outshare_df) :
  #   _df1 = outshare_df
  #
  #   _msk = _df1[tic].eq(_gpo.name[0])
  #   _msk &= _df1[jdt].le(_gpo.name[1])
  #
  #   _df1 = _df1[_msk]
  #
  #   if len(_df1) > 1 :
  #     return _df1.iloc[-1][[jdt , outs]]
  #
  # def _func(_gpo) :
  #   return _find_nearest_date_and_shares_to_each_gp(_gpo , df1)
  #
  # ##
  # gpo = df.groupby([sym , dtf])
  # df2 = gpo.apply(_func)
  #
  # ##
  # df2.to_excel('df2.xlsx')

  ##
  df2 = rdata('df2.xlsx')
  for cn in [dtf , jdt] :
    df2[cn] = df2[cn].apply(isojd)

  ##
  df = df.merge(df2 , how = 'left')

  ##
  ndt = 'NearesetDate2' + dtf
  outsndt = outs + 'On' + ndt

  ren = {
      jdt  : ndt ,
      outs : outsndt ,
      }

  df = df.rename(columns = ren)

  ##
  df['DaysBet'] = df[dtf] - df[ndt]
  df['DaysBet'] = df['DaysBet'].apply(lambda x : x.days)
  df['DaysBet'] = df['DaysBet'].astype('Int64')

  ##
  df['shrout2'] = df[outsndt].astype("Int64") / 10 ** 6
  df = df.drop(columns = outsndt)

  ##
  df['sshrout'] = df['shrout'].eq(df['shrout2'])

  ##
  fshro = 'fshrout'
  msk = df['sshrout'].eq(True)
  df.loc[msk , fshro] = df['shrout']

  ##
  msk = df['shrout2'].notna()
  msk &= df['sshrout'].eq(False)
  msk &= df['DaysBet'].le(100)

  df.loc[msk , fshro] = df['shrout2']

  ##
  msk = df[fshro].isna()
  len(msk[msk])

  ##
  sxl(df , 'out.xlsx')


##


##