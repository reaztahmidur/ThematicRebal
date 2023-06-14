"""
cd "/Volumes/SyntaxFiles/Index/JM back_up/Index_caclulation_proto"
/Library/Frameworks/Python.framework/Versions/3.8/bin/python3 static_pulls_Datastream_api.py
"""


import DatastreamDSWS as dsws
import pandas as pd
import numpy as np
import math

def load_datastream():
    #ds = dsws.Datastream(username = 'jmegill@syntaxindices.com', password = "$ynData51")
    ds = dsws.Datastream(username = 'ZXGB013', password = "PLATE199")

    return ds



def static_request(date1,ric_list, fields_list, datatype, write_df = True):
    ds = load_datastream()
    data_items_amount = len(ric_list)*len(fields_list)
    print(data_items_amount)
    if (data_items_amount <= 100) and (len(ric_list) <= 50):
        ric_str = "".join(['{},'.format(ric) for ric in ric_list])[:-1]
        df = ds.get_data (tickers=ric_str, fields=fields_list, start=date1, kind=0)
        print (df)
    elif (data_items_amount <= 500) and (len(ric_list) <= 250):
        ticker_perrquest =  min(math.floor(100.0 / len(fields_list)),50)
        print()
        reqs =[]
        for num in range(0,len(ric_list),ticker_perrquest):
            ric_str = "".join(['{},'.format(ric) for ric in ric_list[num:num+ticker_perrquest]])[:-1]
            print(ric_str)
            reqs.append(ds.post_user_request(tickers=ric_str, fields=fields_list, start=date1, kind=0))
        df = ds.get_bundle_data(bundleRequest=reqs)
        df = pd.concat(df)

        print(df, 'dataframe &&&&&&&&&&&&&&&&&&&')

    elif (data_items_amount > 500) or (len(ric_list) > 250):
        ticker_perrquest =  min(math.floor(100.0 / len(fields_list)),50)
        ticker_perbundle =  math.floor(500.0 / (len(fields_list)))
        print(ticker_perbundle)

        df = pd.DataFrame()
        count = 0
        for bundle_number in range(0,len(ric_list),ticker_perbundle):
            count += 1
            print(bundle_number,bundle_number+ticker_perbundle,ticker_perrquest)
            print ("loop ", count)

            reqs =[]
            for num in range(bundle_number,min(bundle_number+ticker_perbundle,len(ric_list)),ticker_perrquest):
                ric_str = "".join(['{},'.format(ric) for ric in ric_list[num:num+ticker_perrquest]])[:-1]
                # print(ric_str)
                reqs.append(ds.post_user_request(tickers=ric_str, fields=fields_list, start=date1, kind=0))
            df_list = ds.get_bundle_data(bundleRequest=reqs)
            print (df_list)
            df_temp = pd.DataFrame()

            if df_list:
                # print(df)
                for dataframe1 in df_list:
                    print('datfframe1',dataframe1,type(dataframe1))
                    if df_temp.empty:
                        df_temp = dataframe1
                    else:
                        df_temp = pd.concat([df_temp,dataframe1], axis=0)
                        print('temp',df_temp)

                if df.empty:
                    df = df_temp
                else:
                    df = pd.concat([df,df_temp], axis=0)
            df = df.copy()



    print(df)

    # if len(fields_list) > 1:
    # df = df[df['Instrument'].notna()]
    
    
    if write_df == True:

        df = df.pivot(index=['Dates','Instrument'], columns='Datatype', values='Value').reset_index()
        print(df)
        writer = pd.ExcelWriter('static_fields\\{}_{}.xlsx'.format(date1,datatype))
        df.to_excel(writer,'Sheet1')
        writer.save()
        df.to_csv('static_fields\\{}_{}.csv'.format(date1,datatype))

    else:
        
        return df



if __name__=='__main__':

    datastream_id_filename = 'see_structure\\unique_mic_test.xlsx'
    df = pd.read_excel(datastream_id_filename, sheet_name='Sheet2')
    dups = df[df.duplicated()]
    print(dups)
    datastream_id_list = df['CODE'].tolist()
    print(datastream_id_filename)


    dateslist =  ['2021-11-02']


    fields_list = ['PCUR', 'DEADDT']






    # fields_list = ['DEADDT']
    # dateslist = ['1999-03-31', '2000-09-08','2001-03-09', '2001-09-14','2002-03-08', '2002-09-13']
    # for date2 in dateslist:
    for item in fields_list:
        item_list = [item]
        static_request('2022-06-13', datastream_id_list, item_list, item)
        # for year in range(2005,2015):
        #     static_request('{}-06-30'.format(year), datastream_id_list, item_list, item)
