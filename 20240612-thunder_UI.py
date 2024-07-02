import streamlit as st
import pandas as pd
import numpy as np
import datetime
from functools import partial
import os
import requests
import io
from concurrent.futures import ThreadPoolExecutor

import pandas_ta as ta

# Function to download file from GitHub
def download_file_from_github(url):
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text))

# Function to download multiple files from GitHub and concatenate them
def download_and_concat_files_df(folder_name,endwith):
    response = requests.get(f"https://api.github.com/repos/shohokuno10/Tasi/contents/{folder_name}")
    files = response.json()
    dataframes = pd.DataFrame()
    for file in files:
        if file['name'].endswith(endwith):#.startwith()
            file_url = file['download_url']
            df = download_file_from_github(file_url)
            dataframes = pd.concat([dataframes,df])
    return dataframes

def download_and_concat_files_df_tse(folder_name,endwith):
    response = requests.get(f"https://api.github.com/repos/shohokuno10/Tasi/contents/{folder_name}")
    files = response.json()
    dataframes = pd.DataFrame()
    for file in files:
        if file['name'].endswith(endwith):
            file_url = file['download_url']
            df = download_file_from_github(file_url)
            datatime=file['name'][0:8]
            df.insert(0,'資料日期',datatime)
            if '證券代號' in df.columns:
                df=df[['資料日期','證券代號', '證券名稱', '本益比', '殖利率(%)', '股價淨值比']].rename(columns={'證券代號':'股票代號' ,'證券名稱':'名稱','殖利率(%)':'殖利率'})
            else:
                df=df[['資料日期','股票代號', '股票名稱', '本益比',  '殖利率(%)', '股價淨值比']].rename(columns={'股票名稱':'名稱','殖利率(%)':'殖利率'})
            dataframes = pd.concat([dataframes,df])
    return dataframes

def download_and_concat_files_df_rev(folder_name,endwith):
    response = requests.get(f"https://api.github.com/repos/shohokuno10/Tasi/contents/{folder_name}")
    files = response.json()
    dataframes = pd.DataFrame()
    for file in files:
        if file['name'].endswith(endwith):
            file_url = file['download_url']
            df = download_file_from_github(file_url)
            thismon=file['name'][0:6]
            df.insert(2, 'thismon', thismon)
            dataframes = pd.concat([dataframes,df])
    return dataframes

np.set_printoptions(suppress=True)
pd.set_option('display.float_format', '{:.0f}'.format)
timestart = datetime.datetime.now()

def lineNotifyMessage(token, msg):
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {'message': msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)
    return r.status_code

def nowtimeKBAR(stocno):
    dayk1 = pd.DataFrame()
    file_list = os.listdir("c:/個股拍/" + str(stocno) + "/")
    if file_list and file_list[-1] == thisdate.replace("-", "") + ".csv":
        ticks = pd.read_csv("c:/個股拍/" + str(stocno) + "/" + thisdate.replace("-", "") + ".csv")
        if len(ticks) != 0:
            dayk1 = (ticks.groupby(['stoc', 'date'])
                          .agg({'pric': ['max', 'min', 'first', 'last'], 'volume': 'sum'})
                          .reset_index())
            dayk1.columns = ['stoc', 'date', 'max', 'min', 'ope', 'clo', 'vol']
            dayk1['date'] = pd.to_datetime(dayk1['date'])
    return dayk1

def calculate_kd(kbarst, rev_thistoc_mon, pure_thiswtoc, tracing=1, conditions=None):
    kdpic_st2 = pd.DataFrame()
    kbarst = kbarst.reset_index(drop=True)
    if kbarst['date'].iloc[-1] == pd.Timestamp(thisdate):
        kbarst['per'] = round((kbarst['clo'] - kbarst['clo'].shift(1)) * 100 / kbarst['clo'].shift(1), 2)
        kbarst['std_vol28'] = kbarst['vol'].rolling(28).std()
        kbarst['mean_vol28'] = kbarst['vol'].rolling(28).mean()
        kbarst['mean_vol10'] = kbarst['vol'].rolling(10).mean()
        kbarst['mean_vol20'] = kbarst['vol'].rolling(20).mean()
        kbarst['sma20'] = ta.sma(kbarst['clo'], length=20)
        kbarst['sma60'] = ta.sma(kbarst['clo'], length=60)
        kbarst['ema20'] = ta.ema(kbarst['clo'], length=20)
        kbarst['ema60'] = ta.ema(kbarst['clo'], length=60)
        kbarst['std_30'] = kbarst['clo'].rolling(30).std()
        kbarst['vol_std_30'] = kbarst['vol'].rolling(30).std()
        kbarst['vol_mean_30'] = kbarst['vol'].rolling(30).mean()
        kbarst['atr_30'] = ta.atr(kbarst['max'], kbarst['min'], kbarst['clo'], length=30)
        kbarst['atr_mean_30'] = ta.sma(kbarst['atr_30'], length=30)
        macd_df = ta.macd(kbarst['clo'], fast=12, slow=26, signal=9)
        kbarst['macd'] = macd_df['MACDh_12_26_9']
        stoch_df = ta.stoch(kbarst['max'], kbarst['min'], kbarst['clo'], fastk=9, slowk=5, slowd=5, mamode='ema')
        kbarst['k_pta'] = stoch_df['STOCHk_9_5_5']
        kbarst['d_pta'] = stoch_df['STOCHd_9_5_5']
        bbands_df = ta.bbands(kbarst['clo'], length=20, std=2.5)
        kbarst['bulinup'] = bbands_df['BBU_20_2.5']
        kbarst['bulinmi'] = bbands_df['BBM_20_2.5']
        kbarst['bulinlo'] = bbands_df['BBL_20_2.5']
        kbarst['rsi'] = ta.rsi(kbarst['clo'], length=14)
        adx_df = ta.adx(kbarst['max'], kbarst['min'], kbarst['clo'], length=14)
        kbarst['adx'] = adx_df['ADX_14']
        kbarst = kbarst.reset_index(drop=True)
        kbarst_out = kbarst.copy()

        for i in range(1, tracing + 1):
            if kbarst['vol'].iloc[-60:-1].mean() >= 1:
                if len(kbarst) > 30 and len(rev_thistoc_mon) > 3:
                    condition = (
                        (kbarst['clo'].iloc[-1] > kbarst['sma60'].iloc[-1]) & 
                        (kbarst['mean_vol10'].iloc[-1] > kbarst['mean_vol20'].iloc[-1]) & 
                        (((kbarst['bulinup'].iloc[-1] - kbarst['bulinlo'].iloc[-1]) / kbarst['bulinmi'].iloc[-1]) <= conditions['bollinger_width']) & 
                        (kbarst['rsi'].iloc[-1] < conditions['rsi_max']) & 
                        (kbarst['adx'].iloc[-1] > conditions['adx_min']) &  
                        (kbarst['sma20'].iloc[-1] > kbarst['sma20'].iloc[-2]) & 
                        (kbarst['std_30'].iloc[-1] < kbarst['clo'].mean() * conditions['std_threshold']) &  
                        (kbarst['vol_std_30'].iloc[-1] < kbarst['vol_mean_30'].iloc[-1] * conditions['vol_std_threshold']) &  
                        (kbarst['atr_30'].iloc[-1] < kbarst['atr_mean_30'].iloc[-1] * conditions['atr_threshold'])
                    )
                    if condition:
                        kdpic_st = kbarst.iloc[-1:]
                        kdpic_st = kdpic_st.reset_index(drop=True)
                        kdpic_st['stoc'] = kdpic_st['stoc'].astype(int).astype(str)
                        rev_thistoc_mon['stocnumb'] = rev_thistoc_mon['stocnumb'].astype(str)
                        rev_thistoc_mon['threem'] = ta.sma(rev_thistoc_mon['thisrev'], length=3).round(2).fillna(0).astype(int)
                        rev_thistoc_mon['sixm'] = ta.sma(rev_thistoc_mon['thisrev'], length=6).round(2).fillna(0).astype(int)
                        rev_thistoc_mon = rev_thistoc_mon.reset_index(drop=True)
                        kdpic_st['rev_mon'] = np.where(kdpic_st['date'].dt.day > 12, (str(kdpic_st['date'].iloc[0] - pd.DateOffset(months=1))[0:7].replace('-', '')), (str(kdpic_st['date'].iloc[0] - pd.DateOffset(months=2))[0:7].replace('-', '')))
                        kdpic_rev = pd.merge(kdpic_st, rev_thistoc_mon, left_on=('stoc', 'rev_mon'), right_on=('stocnumb', 'thismon'))
                        kdpic_rev = kdpic_rev.reset_index(drop=True)
                        pure_thiswtoc['股票代號'] = pure_thiswtoc['股票代號'].astype(str)
                        kdpic_rev_pur = pd.merge(kdpic_rev, pure_thiswtoc, how='left', left_on=('stoc', 'date'), right_on=('股票代號', '資料日期2'))
                        kdpic_rev_pur = kdpic_rev_pur.reset_index(drop=True)
                        if len(kdpic_rev_pur) > 0:
                            if not kdpic_rev_pur['股票代號'].isna().any():
                                kdpic_rev_pur = kdpic_rev_pur[kdpic_rev_pur['本益比'] < 20]
                            if len(kdpic_rev_pur) > 0:
                                condition_rev = (
                                    (kdpic_rev_pur['yoy'].iloc[-1] >= -100000) &
                                    (kdpic_rev_pur['threem'].iloc[-1] > kdpic_rev_pur['sixm'].iloc[-1])
                                )
                                if condition_rev:
                                    outq1 = kbarst_out['date'] > kdpic_rev_pur['date'].iloc[0]
                                    outq2 = kbarst_out['clo'] > (kdpic_rev_pur['clo'].iloc[0] * 1.10)
                                    outq3 = kbarst_out['clo'] < (kdpic_rev_pur['clo'].iloc[0] * 0.90)
                                    outtime = kbarst_out[(outq1) & (outq2 | outq3)]
                                    if len(outtime) > 0:
                                        outtime2 = outtime[['date', 'clo', 'per']].rename(columns={'date': 'date_out', 'clo': 'clo_out', 'per': 'per_out'}).iloc[0:1]
                                        kdpic_rev_pur = kdpic_rev_pur.reset_index(drop=True)
                                        outtime2 = outtime2.reset_index(drop=True)
                                        kdpic_rev_pur = pd.concat([kdpic_rev_pur, outtime2], axis=1)
                                        kdpic_st2 = pd.concat([kdpic_st2, kdpic_rev_pur], axis=0)
                                    else:
                                        outtime = kbarst_out.iloc[-1:]
                                        outtime2 = outtime[['date', 'clo', 'per']].rename(columns={'date': 'date_out', 'clo': 'clo_out', 'per': 'per_out'}).iloc[0:1]
                                        kdpic_rev_pur = kdpic_rev_pur.reset_index(drop=True)
                                        outtime2 = outtime2.reset_index(drop=True)
                                        kdpic_rev_pur = pd.concat([kdpic_rev_pur, outtime2], axis=1)
                                        kdpic_st2 = pd.concat([kdpic_st2, kdpic_rev_pur], axis=0)
                    kbarst.drop(kbarst.tail(1).index, inplace=True)
    return kdpic_st2

def run_analysis(tracing, conditions):
    global thisdate
    kbar = download_and_concat_files_df(folder_name='kbar', endwith='csv')
    kbar['date']=pd.to_datetime(kbar['date'])

    stocno = kbar['stoc'].unique()
    thisdate = kbar['date'].max().date()
    
    pure_otc_all = download_and_concat_files_df(folder_name='淨值', endwith="櫃淨值.csv")

    pure_otc_all = pure_otc_all.reset_index(drop=True)
    pure_otc_all['資料日期2'] = (pure_otc_all['資料日期'].dropna().astype(int) + 19110000).astype(str)
    pure_otc_all['資料日期2'] = pure_otc_all['資料日期2'].str[0:4]+'-'+pure_otc_all['資料日期2'].str[4:6]+'-'+pure_otc_all['資料日期2'].str[6:8]
    pure_otc_all['資料日期2'] = pd.to_datetime(pure_otc_all['資料日期2'],format='%Y-%m-%d', utc=False,errors='coerce')
    pure_otc_all = pure_otc_all[['資料日期2','股票代號', '名稱', '本益比',  '殖利率', '股價淨值比']]
    
    pure_tse_all = download_and_concat_files_df_tse('淨值', '市淨值.csv')

    pure_tse_all = pure_tse_all.reset_index(drop=True)    
    pure_tse_all['資料日期2'] = pd.to_datetime(pure_tse_all['資料日期'], utc=False,errors='coerce')    
    pure_tse_all = pure_tse_all.drop('資料日期',axis=1)    

    pure = pd.concat([pure_otc_all,pure_tse_all])
    pure = pure.reset_index(drop=True)    
    revanue = download_and_concat_files_df_rev('營收', '.csv')
    revanue = revanue.sort_values(by=['公司代號','thismon'])
    revanue = revanue[~revanue['公司代號'].isin(['全部國內上櫃公司合計','全部國內上市公司合計'])]
    revanue = revanue.reset_index(drop=True)
    revanue['公司代號'] = revanue['公司代號'].astype(int)
    revanue = revanue[['公司代號',
     '公司名稱',
     'thismon',
     '當月營收',
     '上月比較增減(%)',
     '去年同月增減(%)',
     ]]  
    revanue = revanue.rename(columns={'公司代號':'stocnumb','公司名稱':'stocname','當月營收':'thisrev','上月比較增減(%)':'mom','去年同月增減(%)':'yoy'})

    func = partial(calculate_kd, tracing=tracing, conditions=conditions)
    params = [(kbar[kbar['stoc'] == stoc], revanue[revanue['stocnumb'] == stoc], pure[pure['股票代號'] == stoc]) for stoc in stocno]

    with ThreadPoolExecutor() as executor:
        result_list = list(executor.map(lambda p: func(*p), params))

    kdpickor = pd.concat(result_list)
    kdpick = kdpickor.reset_index(drop=True)
    idx = kdpick.groupby(['stoc', 'date'])['date'].idxmin()
    kdpick = kdpick.loc[idx]
    kdpick['benefit'] = kdpick['clo_out'] - kdpick['clo']
    kdpick['benefitrat'] = kdpick['benefit'] / kdpick['clo']
    kdpick['date'] = kdpick['date'].dt.date
    kdpick['date_out'] = kdpick['date_out'].dt.date
    kdpick = kdpick[(kdpick['per_out'] < 10) 
                    & (kdpick['per_out'] > -10) 
                    & (kdpick['benefitrat'] > -0.2) 
                    & (kdpick['benefitrat'] < 0.25) 
                    ]

    if len(kdpick) != 0:
        idid_url = 'https://raw.githubusercontent.com/shohokuno10/Tasi/main/%E5%80%8B%E8%82%A1%E8%99%9F%E7%94%A2%E6%A5%AD2.csv'
        idid = download_file_from_github(idid_url)
        idid['stockid'] = idid['stockid'].astype(str)
        kdpick_id = kdpick.merge(idid, left_on='stoc', right_on='stockid', how='left')
        kdpick_id['holdingday'] = (pd.to_datetime(kdpick_id['date_out']) - pd.to_datetime(kdpick_id['date'])).dt.days
        winrate = (kdpick['benefit'] > 0).sum() * 100 / (kdpick['benefit']).count()

        db1 = kdpick_id.copy()
        db2 = kdpick_id[~kdpick_id['stockkin'].isin(['金控', '銀行', '保險', '證券', '營建'])]
        winrate2 = (db2['benefit'] > 0).sum() * 100 / (db2['benefit']).count()
        result = f'勝率 : {int(winrate)}%\n扣金控勝率：{int(winrate2)}%'
    else:
        result = '沒有出手'
    st.write('資料日期'+str(thisdate))
    st.write("分析結果")
    st.write(result)
    st.write('出手'+str(len(db1))+'次')
    st.write(db1)

def main():
    st.title("股票分析系統")

    tracing = st.number_input("回測天數", min_value=1, value=1, step=1)
    bollinger_width = st.number_input("布林帶寬度 (如0.05)", min_value=0.01, value=0.05, step=0.01)
    rsi_max = st.number_input("RSI最大值 (如70)", min_value=0.01, value=70.0, step=0.01)
    adx_min = st.number_input("ADX最小值 (如25)", min_value=0.01, value=25.0, step=0.01)
    std_threshold = st.number_input("價格標準差閾值 (如0.01)", min_value=0.01, value=0.01, step=0.01)
    vol_std_threshold = st.number_input("成交量標準差閾值 (如0.2)", min_value=0.01, value=0.2, step=0.01)
    atr_threshold = st.number_input("ATR閾值 (如0.2)", min_value=0.01, value=0.2, step=0.01)

    if st.button("開始分析"):
        conditions = {
            'bollinger_width': bollinger_width,
            'rsi_max': rsi_max,
            'adx_min': adx_min,
            'std_threshold': std_threshold,
            'vol_std_threshold': vol_std_threshold,
            'atr_threshold': atr_threshold
        }
        run_analysis(tracing, conditions)

if __name__ == "__main__":
    main()
