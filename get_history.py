from datetime import datetime, timedelta, date
import numpy as np
import requests, json
import pandas as pd

# 13年(民99-民112)台股資料需時9小時，
# 若讀者未來想加速: 可改採用cuDF，目前cuDF只支援Linux, Windows用戶可使用WSL開再安裝cuDF。

# 印出本程式介紹
def Intro():
    print("*********************************************************************************************")
    print("| TW-stock v1.0 (Dec.24,2023)                                                               |")
    print("| 作者: k66(Lana Chen)                                                                      |")
    print("| 描述: 根據台股代碼 (k66提供的twse_stocks_id.h5)從證交所(twse)下載台灣上市股8檔資料。      |")
    print("|                                                                                           |")
    print("| Log:                                                                                      |")
    print("| v1.0: 共1022支上市股，產業共32類。(Dec.24,2023)                                           |")
    print("|                                                                                           |")
    print("| 贊助我Buy me a coffee:                                                                    |")
    print("| 贊助連結: https://www.buymeacoffee.com/k66inthesky                                        |")
    print("| 為感謝支持者，我會額外提供2支程式:                                                        |")
    print("| backtest.py(回測程式，且能完美銜接此程式)及get_ids.py(能因應新上市股)。                   |")
    print("*********************************************************************************************")
    print()
    print()
    

# 前處理: 日期資料
def PreProcess():
    # TODO: 請依需求自行修改日期，2010年以前也適用
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 2, 1)# datetime(2024, 2, 1)

    # Initialize the list to store dates
    weekdays = []

    # Iterate over the date range
    current_date = start_date
    while current_date <= end_date:
        # Check if the current date is a weekday (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Monday to Friday
            # Add the date to the list in the specified format
            weekdays.append(current_date.strftime("%Y%m%d"))
        # Move to the next day
        current_date += timedelta(days=1)

    # print(weekdays[-10:],len(weekdays))  # Display the first 10 dates for verification

    # 證交所按月抓取股票資料
    df=pd.read_hdf('twse_stocks_id.h5',mode="r",index=False)
    print('產業共有幾類: ', df['industry'].nunique())
    unique_industry_categories = df['industry'].unique()
    # print(unique_industry_categories) # 將所有產業別印出
    df = df.sort_values(by='industry') # 讀來的時候按理已經按產業排序，此處多做一次防呆

    sorted_df=df.sort_values(by=['industry'])

    # 因為現今證交所歷史資料從民99年1月4日開始，https://www.twse.com.tw/zh/trading/historical/stock-day.html
    # 市場開休市日期查詢:https://www.twse.com.tw/pcversion/zh/holidaySchedule/holidaySchedule
    # 證交所公布之補行交易日
    # TODO: 提醒! 隨著日期區間更動，記得也要更動comtradedays及holidays!
    comtradedays=[]
    holidays=['20230102']
    # 證交所公布之休市日
    '''
    comtradedays=[
                '20170110','20180117','20170606'
                ]
    holidays=[
            '20100101','20100211','20100212','20100215','20100216','20100217','20100218','20100219','20100405','20100616','20100922',
            '20110103','20110131','20110201','20110202','20110203','20110204','20110207','20110228','20110404','20110405','20110502','20110606','20110912','20111010',
            '20120102','20120119','20120120','20120123','20120124','20120125','20120126','20120127','20120227','20120228','20120404','20120501','20120930','20121010','20121231',
            '20130101','20130207','20130208','20130211','20130212','20130213','20130214','20130215','20130228','20130404','20130405','20130501','20130612','20130919','20130920','20131010',
            '20140101','20140128','20140129','20140130','20140131','20140203','20140204','20140228','20140404','20140501','20140602','20140908','20141010',
            '20150101','20150102','20150216','20150217','20150218','20150219','20150220','20150223','20150224','20150227','20150403','20150406','20150501','20150619','20150928','20151009',
            '20160101','20160104','20160204','20160205','20160208','20160209','20160210','20160211','20160212','20160215','20160229','20160404','20160405','20160502','20160609','20160915','20160916','20161010',
            '20170102','20170103','20170125','20170126','20170127','20170130','20170131','20170201','20170202','20170227','20170228','20170403','20170404','20170501','20170529','20170530','20171004','20171009','20171010',
            '20180101','20180213','20180214','20180215','20180216','20180219','20180220','20180221','20180228','20180404','20180405','20180406','20180501','20180618','20180924','20181010','20181231',
            '20190101','20190102','20190130','20190204','20190205','20190206','20190207','20190208','20190211','20190228','20190301','20190404','20190405','20190501','20190607','20190913','20191010','20191011',
            '20200101','20200102','20200121','20200122','20200123','20200124','20200127','20200128','20200129','20200228','20200402','20200403','20200501','20200625','20200626','20201001','20201002','20201009',
            '20210101','20210208','20210209','20210210','20210211','20210212','20210215','20210216','20210217','20210301','20210402','20210405','20210430','20210614','20210920','20210921','20211011','20211231',
            '20220127','20220128','20220131','20220201','20220202','20220203','20220204','20220228','20220404','20220405','20220502','20220603','20220909','20221010',
            '20230102','20230103','20230118','20230119','20230120','20230123','20230124','20230125','20230126','20230127','20230227','20230228','20230403','20230404','20230501','20230622','20230623','20230929','20231009','20231010'
            ]
    '''
    # marketdays = weekdays & !holidays
    # 工作日 = 一般平日+補交易日
    # 交易日 = 工作日和非假日的交集
    weekdays = weekdays+comtradedays
    marketdays = RemoveCommonElements(weekdays,holidays)

    # 日期，每一單位為一個月
    # TODO: request以下url，請依需求自行修改日期
    # months = pd.date_range('2010-01-01','2023-12-31', freq='MS').strftime("%Y%m%d").tolist()   
    months = pd.date_range(start_date,end_date, freq='MS').strftime("%Y%m%d").tolist()

    return df,sorted_df, months


# 前處理用到的演算法，利用日期的嚴格遞增特性優化。
def RemoveCommonElements(list1,list2):
    # 從List1從刪與list2共同元素，利用list1,list2已知的日期屬於嚴格遞增特性。
    # marketdays = weekdays & !holidays
    # 優化後，時間複雜度由原本O(MN)變成O(M+N)。
    # M: 從自行設定的開始日至截止日，共M天，N: 共N隻上市股(根據k66提供之twse_stocks_id.h5)。
    p1,p2,res=0,0,[]#pointer1,pointer2,result
    while p1 < len(list1) and p2 < len(list2):
        if list1[p1]<list2[p2]:
            res.append(list1[p1])
            p1+=1
        elif list1[p1]>list2[p2]:
            p2+=1
        else:
            p1+=1
            p2+=1
    if p1<len(list1):
        res.extend(list1[p1:])
    return res

def Crawl(df,sorted_df,months):
    # 下載所有台股上市股
    # for i in range(len(sorted_df)):
    for i in range(100): # Demo抓前100支個股就好
        # 每一隻股票皆獨立存為一檔案
        id = sorted_df['stocks_id'][i]
        print('開始爬蟲台股，代碼: ',id)
        print('進度: ', i/100*100, "%(",i,"/",100,")")
        # print('進度: ', i/len(sorted_df)*100, "%(",i,"/",len(sorted_df),")")
        # 下載證交所資料時，RESTful參數日期單位為一個月，故此處才用月，但其實行為會抓整個月內的日k。
        df=pd.DataFrame()
        for month in months:
            try:
                # TODO: 1.改為request(selenium)抓各月csv檔，2.try except 3.存至專屬資料夾 4.根據證券代碼整理這些csv檔
                url='https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date='+month+'&stockNo='+id
                html = requests.get(url)
                content = json.loads(html.text)
                stock_data = content['data']
                col_name = content['fields']
                df_onemonth = pd.DataFrame(data=stock_data, columns=col_name)
                df = pd.concat([df,df_onemonth], ignore_index = True)
            except:
                print("The stock in this month has no data: "+month,". Please change the date range.")
            # TODO: [使用者須留意] 若只做一個月，某些個股(如代號1435的2023/1月因無資料)會跳except，只要略過此範圍即可。
        
        df.columns = ['日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']
        # 把日期改為西元
        for row in range(df.shape[0]):
            l = df.iloc[row,0].split('/')
            df.iloc[row, 0] = date( int(l[0].lstrip())+1911, int(l[1]), int(l[2]) )
        # 把股數或價格等帶有逗號的string轉換成float
        for col in [1, 2, 3, 4, 5, 6, 8]: #第0行是日期所以不用轉換
            for row in range(df.shape[0]):
                tmp=df.iloc[row,col].replace(',', '')
                try:
                    df.iloc[row, col] = float(tmp) # 證交所將缺用--表示
                except ValueError:
                    #證交所將缺用--表示，故我們將其改為NaN
                    df.iloc[row, col] = np.nan
                    print(tmp)
        # print(df[-10:-5]) # 螢幕上印出其中5筆，如不想要請註解之。
        df.to_csv("stocks/"+id+".csv",mode="w",index=False, encoding="UTF-8")
        # 此資料不建議存成hd5，此資料存成csv反而有優勢
        # df.to_hdf("stocks/"+id+".h5",mode="w",format = 'fixed', key='df',index=False, encoding="UTF-8")
        
        print('[完成]台股代碼: ',id)
    print('[完成]下載所有台股歷年資料')


# 聯絡&贊助資訊
def Contact():
    print("*********************************************************************************************")
    print("| Hi我是k66(Lana), 感謝您使用TW-stock v1.0，為支持我後續更多創作，希望獲得您的寶貴贊助!    |")
    print("|                                                                                          |")
    print("|                                                                                          |")
    print("| 贊助我Buy me a coffee:                                                                   |")
    print("| 贊助連結: https://www.buymeacoffee.com/k66inthesky/e/216935                              |")
    print("| 為感謝支持者，我會額外提供2支程式:                                                       |")
    print("| backtest.py(回測程式，且能完美銜接此程式)及get_ids.py(能因應新上市股)。                  |")
    print("|                                                                                          |")
    print("|                                                                                          |")
    print("| 若使用上遇到問題，歡迎Github開issue聯繫我:                                               |")
    print("| https://github.com/k66inthesky/TW-stock/issues                                           |")
    print("*********************************************************************************************")
    print()
    print()

def main():
    Intro()
    df,sorted_df,months = PreProcess()
    print("前處理完成!")
    Crawl(df,sorted_df,months)
    print("整隻程式運行完成!")
    Contact()



if __name__ == '__main__':
    main()
