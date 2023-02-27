import base64
import io


import dash
from dash import no_update
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from dash import dash_table

import xlsxwriter

import pandas as pd
import numpy as np


#from datetime import date,timedelta, datetime

def parse_contents(contents, filename, date):

    #print(contents,filename, date)


    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)

    try:
        if 'csv' in filename:
            #print("XXXXXX")
            # Assume that the user uploaded a CSV file
            df_ws = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')), header=None)[0]



        else:

            df_ws=['ERROR']



    except Exception as e:
        print('ERROR',e)
        df_ws=['ERROR']

    return df_ws


def text_updater(text, path, operation):
    file_update=open(path, operation)
    file_update.write(text)
    file_update.close()
    return file_update


def LTE_LNCEL_Report(url, conn, conn_PM, s3, WS_dict,  start_d, end_d, log_control, time_agg, *scheduled):

    from datetime import date,timedelta, datetime
    end_d_sql=(datetime.strptime(end_d, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')

    mem_usage_MB=-1

    WS=[WS_dict[i] for i in WS_dict]

    print(WS)

    #API url
    import requests
    import json
    import pandas as pd
    import time

    start_time = time.time()

    # ## LTE


    #iniciar fechas
    today = date.today()

    #start_d=(today + timedelta(days = -hist_days)).strftime("%Y-%m-%d")

    print(start_d,'\n')



    kpis=['LTE_5239a','LTE_1072a','LTE_5569a','LTE_753c','LTE_5218f','LTE_5116a','LTE_5003a','LTE_5118a','LTE_5017a','LTE_5123b','LTE_5043b','LTE_5058b','LTE_5240a','LTE_1078a','LTE_5114a','LTE_5289d','LTE_5292d','LTE_5212a','LTE_5213a','LTE_5427c','M8005C306','M8005C307','M8005C308','M8005C309','LTE_1339a','LTE_1067c','LTE_717b','LTE_5025h','LTE_5218g','LTE_291b','LTE_5023h','LTE_6265a','LTE_249b','LTE_250b']
    print('Ejecutando - 1')
    kpis1=kpis[:int(len(kpis)/2)]
    kpis2=kpis[int(len(kpis)/2):]


    obj = {
    "tech": "LTE",
    "kpi_id": ",".join(kpis1),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"sector_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    print(obj)

    print('Ejecutando')
    x = requests.post(url, json = obj)
    data = json.loads(x.text)
    print('Ejecutado LTE p1')
    DATA_LTE = pd.DataFrame.from_dict(data)
    print(DATA_LTE.shape)


    obj = {
    "tech": "LTE",
    "kpi_id": ",".join(kpis2),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"sector_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    print(obj)

    print('Ejecutando')
    x = requests.post(url, json = obj)
    data = json.loads(x.text)
    print('Ejecutado LTE p2')
    DATA_LTE_2 = pd.DataFrame.from_dict(data)
    print(DATA_LTE_2.shape)


    
    print("Merge Listo")

    if DATA_LTE.shape[0]>0:

        DATA_LTE=DATA_LTE.merge(DATA_LTE_2, left_on=['Date','Cell'], right_on=['Date','Cell'])


        DATA_LTE['Period start time']=DATA_LTE['Date']
        DATA_LTE['LNCEL name']=DATA_LTE['Cell']
        DATA_LTE['LNBTS name']=DATA_LTE['Cell'].apply(lambda x: x[:x.find("_")])
        DATA_LTE['MRBTS/SBTS name']="MRBTS-"+DATA_LTE['LNBTS name']

        columns_report=['Period start time','MRBTS/SBTS name','LNBTS name','LNCEL name']+kpis

        #Rellenar columnas faltantes

        missing_col=[]

        for column in columns_report:

            if column not in DATA_LTE.columns:
                missing_col.append(column)

        for column in missing_col:
            DATA_LTE[column]=np.nan

        print("Data Lista")

        #writer = pd.ExcelWriter(f'LTE_FL16A_NPO_Monitoring_V4-RSLTE-LNCEL-{time_agg}_{log_control}.xlsx',  engine='xlsxwriter')
        #DATA_LTE[columns_report].sort_values(by=['LNCEL name','Period start time']).to_excel(writer, index=False, sheet_name='data')



        if scheduled:
            print("Llamada por funcion scheduler")
            log_control_sch=datetime.strftime(datetime.today() , '%Y-%m-%d %HH-%MM')
            
        else:

            log_control_sch=log_control
            log_control=datetime.strftime(datetime.today() , '%Y-%m-%d')

        DATA_LTE[columns_report].sort_values(by=['LNCEL name','Period start time']).to_excel(f'LTE_FL16A_NPO_Monitoring_V4-RSLTE-LNCEL-{time_agg}_{log_control}.xlsx', index=False, engine="xlsxwriter")
        print("Excel Listo")

        mem_usage_MB=DATA_LTE.memory_usage(deep=True).sum()/1000000

        s3.upload_file(f'LTE_FL16A_NPO_Monitoring_V4-RSLTE-LNCEL-{time_agg}_{log_control}.xlsx', 'claro-colombia-production' , f'Reports/LTE_700/{log_control}/LTE_FL16A_NPO_Monitoring_V4-RSLTE-LNCEL-{time_agg}_{log_control_sch}.xlsx')
        print("S3 Listo")
        import os
        os.remove(f'LTE_FL16A_NPO_Monitoring_V4-RSLTE-LNCEL-{time_agg}_{log_control}.xlsx')

    return f"Total Time: {round((time.time()-start_time)/60,2)} minutes", mem_usage_MB






def LTE_LNBTS_Report(url, conn, conn_PM, s3, WS_dict, start_d, end_d, log_control, time_agg, *scheduled):

    from datetime import date,timedelta, datetime

    mem_usage_MB=-1
    end_d_sql=(datetime.strptime(end_d, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
    #print(hist_days, url, WS_dict)

    WS=[WS_dict[i] for i in WS_dict]

    print(WS)

    #API url
    import requests
    import json
    import pandas as pd
    import time

    start_time = time.time()

    # ## LTE



    #iniciar fechas
    today = date.today()

    #start_d=(today + timedelta(days = -hist_days)).strftime("%Y-%m-%d")

    print(start_d,'\n')




    sql_str="""
    select distinct SUBSTR(moDistName, 1,position(moDistName,'/LNCEL')-1) as mo_distname,
    site_name as name
    from LTE_Site_Database t
    inner join (
    select sector_name, max(export_date) as MaxDate
    from LTE_Site_Database
    group by sector_name
    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate
    FORMAT TSVWithNamesAndTypes
    """


    BL_LNBTS = pd.read_sql(sql_str, conn_PM)  

    BL_LNBTS.drop_duplicates(inplace=True)

    print(BL_LNBTS)




    sites_mo=BL_LNBTS.merge(pd.DataFrame(data=WS, columns=['Site']), left_on='name', right_on='Site')['mo_distname'].to_list()

    print(sites_mo)

    #TWAMP

    str_sql=f"""
    select    

    DATE_TRUNC ('{time_agg.lower()}',measTime, 'America/Bogota') as measTime,


    distName,

    SUM(avgRTT_15Min) as avgRTT_15Min,
    SUM(maxRTT_15Min) as maxRTT_15Min,
    SUM(minRTT_15Min) as minRTT_15Min,
    SUM(lostTwampMessages) as lostTwampMessages,
    SUM(txTwampMessages) as txTwampMessages

    from 

    (select * from

    LTE_Counters.LTE_TWAMP_Stats       
                    
    where 
    DATE_TRUNC ('{time_agg.lower()}',measTime, 'America/Bogota') >='{start_d}' and
    DATE_TRUNC ('{time_agg.lower()}',measTime, 'America/Bogota') <'{end_d_sql}'


    and (
    distName like '{"%%' or distName like '".join(sites_mo)}/%%'
    )

    LIMIT 1 by measTime, measInterval, distName, extendedDistName
    SETTINGS asterisk_include_alias_columns = 1) a

    group by distName, measTime

    order by measTime


    """

    LTE_TWAMP_Stats = pd.read_sql(str_sql, conn_PM)




    # In[11]:


    #Convertir a numero los campos

    for counter in LTE_TWAMP_Stats.columns[2:]:
        try:
            LTE_TWAMP_Stats[counter]=LTE_TWAMP_Stats[counter].astype(np.int64)
        except:
            LTE_TWAMP_Stats[counter]=LTE_TWAMP_Stats[counter].astype(float)

    LTE_TWAMP_Stats['distName_p']=LTE_TWAMP_Stats['distName'].apply(lambda x: x[:x.find('FTM-')-1])


    # In[12]:


    LTE_TWAMP_Stats_LNBTS=LTE_TWAMP_Stats.drop('distName', axis=1).groupby(['measTime','distName_p']).sum().reset_index().merge(BL_LNBTS, right_on='mo_distname', left_on='distName_p').drop(['distName_p','mo_distname'], axis=1)


    # In[13]:


    LTE_TWAMP_Stats_LNBTS['LTE_1652a']=100*(LTE_TWAMP_Stats_LNBTS['txTwampMessages']-LTE_TWAMP_Stats_LNBTS['lostTwampMessages'])/LTE_TWAMP_Stats_LNBTS['txTwampMessages']

    LTE_TWAMP_Stats_LNBTS['M51132C0']=LTE_TWAMP_Stats_LNBTS['avgRTT_15Min']
    LTE_TWAMP_Stats_LNBTS['M51132C1']=LTE_TWAMP_Stats_LNBTS['maxRTT_15Min']

    LTE_TWAMP_Stats_LNBTS=LTE_TWAMP_Stats_LNBTS[['measTime','name','LTE_1652a','M51132C0','M51132C1']]
    LTE_TWAMP_Stats_LNBTS.columns=['Date','Site','LTE_1652a','M51132C0','M51132C1']


    # In[14]:




    #iniciar fechas
    today = date.today()

    #start_d=(today + timedelta(days = -hist_days)).strftime("%Y-%m-%d")

    kpis=['LTE_5240a','LTE_1472a','M8014C6','LTE_5408a','LTE_1473a','LTE_5058b']

    obj = {
    "tech": "LTE",
    "kpi_id": ",".join(kpis),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"site_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }


    x = requests.post(url, json = obj)
    data = json.loads(x.text)
    DATA_LTE = pd.DataFrame.from_dict(data)
    print(DATA_LTE.shape)

    if DATA_LTE.shape[0]>0:

        # In[15]:


        DATA_LTE=DATA_LTE.merge(LTE_TWAMP_Stats_LNBTS, left_on=['Date','Site'], right_on=['Date','Site'], how='left')

        DATA_LTE['Period start time']=DATA_LTE['Date']
        DATA_LTE['LNBTS name']=DATA_LTE['Site']
        DATA_LTE['MRBTS name']="MRBTS-"+DATA_LTE['LNBTS name']

        columns_report=['Period start time','MRBTS name','LNBTS name']+kpis+['LTE_1652a','M51132C0','M51132C1']


        # In[16]:

        #Rellenar columnas faltantes

        missing_col=[]

        for column in columns_report:

            if column not in DATA_LTE.columns:
                missing_col.append(column)

        for column in missing_col:
            DATA_LTE[column]=np.nan

        if scheduled:
            print("Llamada por funcion scheduler")
            log_control_sch=datetime.strftime(datetime.today() , '%Y-%m-%d %HH-%MM')
            
        else:
            log_control_sch=log_control
            log_control=datetime.strftime(datetime.today() , '%Y-%m-%d')


        mem_usage_MB=DATA_LTE.memory_usage(deep=True).sum()/1000000
        DATA_LTE[columns_report].sort_values(by=['LNBTS name','Period start time']).to_excel(f'Fallas_TX_LTE-RSLTE-LNBTS-{time_agg}_{log_control}.xlsx', index=False, engine='xlsxwriter')

        #writer = pd.ExcelWriter(f'Fallas_TX_LTE-RSLTE-LNBTS-{time_agg}_{log_control}.xlsx',  engine='xlsxwriter')
        #DATA_LTE[columns_report].sort_values(by=['LNBTS name','Period start time']).to_excel(writer, index=False, sheet_name='data')




    #**********
        s3.upload_file(f'Fallas_TX_LTE-RSLTE-LNBTS-{time_agg}_{log_control}.xlsx', 'claro-colombia-production' , f'Reports/LTE_700/{log_control}/Fallas_TX_LTE-RSLTE-LNBTS-{time_agg}_{log_control_sch}.xlsx')
        
        import os
        os.remove(f'Fallas_TX_LTE-RSLTE-LNBTS-{time_agg}_{log_control}.xlsx')

    return f"Total Time: {round((time.time()-start_time)/60,2)} minutes", mem_usage_MB







def UMTS_WCEL_Report(url, conn, conn_PM, s3, WS_dict, start_d, end_d, log_control, time_agg, *scheduled):
    from datetime import date,timedelta, datetime

    mem_usage_MB=-1

    end_d_sql=(datetime.strptime(end_d, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
    #print(hist_days, url, WS_dict)

    WS=[WS_dict[i] for i in WS_dict]

    print(WS)

    #API url
    import requests
    import json
    import pandas as pd
    import time

    start_time = time.time()



    #iniciar fechas
    today = date.today()

    #start_d=(today + timedelta(days = -hist_days)).strftime("%Y-%m-%d")

    print(start_d,'\n')



    kpis=['RNC_183c','RNC_231d','RNC_217g','RNC_5505b','RNC_2697a','RNC_5093b','RNC_280d','RNC_19a','M1006C128','M1006C129','M1006C130','M1006C131','M1006C132','M1006C133','M1006C134','M1006C135','M1006C136','M1006C137','M1006C138','M1006C139','M1006C140','M1006C141','M1006C142','M1006C143','usuarios_dch_ul_ce_new','usuarios_dch_dl_ce_new','RNC_1686a','RNC_920b','RNC_605b','RNC_645c','RNC_1687a','RNC_921c','RNC_913b','RNC_1036b','RNC_1254a','M1022C71','RNC_1255c','RNC_931a','RNC_5043a','RNC_5053a','RNC_339b','RNC_3124a','RNC_3125a','RNC_706b','avgprachdelay','RNC_609a','RNC_1879e']

    kpis1=kpis[:int(len(kpis)/2)]
    kpis2=kpis[int(len(kpis)/2):]

    
    print('Ejecutando - 1')

    obj = {
    "tech": "UMTS",
    "kpi_id": ",".join(kpis1),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"sector_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    #print(obj)

    

    x = requests.post(url, json = obj)
    data = json.loads(x.text)
    print('Ejecutado p1 UMTS')
    DATA_UMTS = pd.DataFrame.from_dict(data)

    obj = {
    "tech": "UMTS",
    "kpi_id": ",".join(kpis2),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"sector_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    #print(obj)

    x = requests.post(url, json = obj)
    data = json.loads(x.text)
    print('Ejecutado p2 UMTS')
    DATA_UMTS_2 = pd.DataFrame.from_dict(data)

    


    if DATA_UMTS.shape[0]>0:

        DATA_UMTS=DATA_UMTS.merge(DATA_UMTS_2, left_on=['Date','Cell'], right_on=['Date','Cell'])
            
 
        sql_str="""
        select rnc_name as RNC_name, 
        extractAll(moDistName, '([0-9]+)')[1] as RNC_id,
        extractAll(moDistName, '([0-9]+)')[2] as WBTS_id,
        extractAll(moDistName, '([0-9]+)')[3] as WCEL_id,
        sector_name as name
        from UMTS_Site_Database t
        inner join (
        select sector_name, max(export_date) as MaxDate
        from UMTS_Site_Database
        group by sector_name
        ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate
        FORMAT TSVWithNamesAndTypes
        """

        BL_WCEL = pd.read_sql(sql_str, conn_PM) 



        BL_WCEL.drop_duplicates(inplace=True)

        DATA_UMTS=BL_WCEL.merge(DATA_UMTS, left_on='name', right_on='Cell')


        print("DATA UMTS:", DATA_UMTS.shape)



        DATA_UMTS['Period start time']=DATA_UMTS['Date']
        DATA_UMTS['PLMN name']="PLMN"
        DATA_UMTS['RNC name']=DATA_UMTS['RNC_name']
        DATA_UMTS['WBTS name']=DATA_UMTS['Cell'].apply(lambda x: x[:x.find("_")])
        DATA_UMTS['WBTS ID']=DATA_UMTS['WBTS_id']
        DATA_UMTS['WCEL name']=DATA_UMTS['Cell']
        DATA_UMTS['WCEL ID']=DATA_UMTS['WCEL_id']

        DATA_UMTS['RNC_2697a']=np.nan

        columns_report=['Period start time','PLMN name','RNC name','WBTS name','WBTS ID','WCEL name']+kpis

        #Rellenar columnas faltantes

        missing_col=[]

        for column in columns_report:

            if column not in DATA_UMTS.columns:
                missing_col.append(column)

        for column in missing_col:
            DATA_UMTS[column]=np.nan

       


        #writer = pd.ExcelWriter(f'WCDMA17_NPO_Monitoring_V4-RSRAN-WCEL-{time_agg}_{log_control}.xlsx',  engine='xlsxwriter')
        #DATA_UMTS[columns_report].sort_values(by=['WCEL name','Period start time']).to_excel(writer, index=False, sheet_name='data')

        if scheduled:
            print("Llamada por funcion scheduler")
            log_control_sch=datetime.strftime(datetime.today() , '%Y-%m-%d %HH-%MM')
            
        else:
            log_control_sch=log_control
            log_control=datetime.strftime(datetime.today() , '%Y-%m-%d')


        mem_usage_MB=DATA_UMTS.memory_usage(deep=True).sum()/1000000
        DATA_UMTS[columns_report].sort_values(by=['WCEL name','Period start time']).to_excel(f'WCDMA17_NPO_Monitoring_V4-RSRAN-WCEL-{time_agg}_{log_control}.xlsx', index=False, engine='xlsxwriter')

        s3.upload_file(f'WCDMA17_NPO_Monitoring_V4-RSRAN-WCEL-{time_agg}_{log_control}.xlsx', 'claro-colombia-production' , f'Reports/LTE_700/{log_control}/WCDMA17_NPO_Monitoring_V4-RSRAN-WCEL-{time_agg}_{log_control_sch}.xlsx')
        
        import os
        os.remove(f'WCDMA17_NPO_Monitoring_V4-RSRAN-WCEL-{time_agg}_{log_control}.xlsx')

    return f"Total Time: {round((time.time()-start_time)/60,2)} minutes", mem_usage_MB










def UMTS_WBTS_Report(url, conn, conn_PM, s3, WS_dict, start_d, end_d, log_control, time_agg, *scheduled):
    from datetime import date,timedelta, datetime
    end_d_sql=(datetime.strptime(end_d, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
    #print(hist_days, url, WS_dict)
    mem_usage_MB=-1
    WS=[WS_dict[i] for i in WS_dict]

    print(WS)

    #API url
    import requests
    import json
    import pandas as pd
    import time

    start_time = time.time()





    #iniciar fechas
    today = date.today()

    #start_d=(today + timedelta(days = -hist_days)).strftime("%Y-%m-%d")

    print(start_d,'\n')

    sql_str="""
    select rnc_name as RNC_name, 
    extractAll(moDistName, '([0-9]+)')[1] as RNC_id,
    extractAll(moDistName, '([0-9]+)')[2] as WBTS_id,
    extractAll(moDistName, '([0-9]+)')[3] as WCEL_id,
    sector_name as name
    from UMTS_Site_Database t
    inner join (
    select sector_name, max(export_date) as MaxDate
    from UMTS_Site_Database
    group by sector_name
    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate
    FORMAT TSVWithNamesAndTypes
    """

    BL_WCEL = pd.read_sql(sql_str, conn_PM) 


    BL_WCEL.drop_duplicates(inplace=True)

    print(BL_WCEL.shape)



    sql_str="""
    select distinct SUBSTR(moDistName, 1,position(moDistName,'/WCEL')-1) as mo_distname,
    site_name as name
    from UMTS_Site_Database t
    inner join (
    select sector_name, max(export_date) as MaxDate
    from UMTS_Site_Database
    group by sector_name
    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate
    FORMAT TSVWithNamesAndTypes
    """


    BL_WBTS = pd.read_sql(sql_str, conn_PM)  


    BL_WBTS.drop_duplicates(inplace=True)

    print(BL_WBTS.shape)

    sites_mo=BL_WBTS.merge(pd.DataFrame(data=WS, columns=['Site']), left_on='name', right_on='Site')['mo_distname'].to_list()




    str_sql=f"""
    select    

    DATE_TRUNC ('{time_agg.lower()}',measTime, 'America/Bogota') as measTime,

    distName,


    SUM(avgRTT_15Min) as avgRTT_15Min,
    SUM(maxRTT_15Min) as maxRTT_15Min,
    SUM(minRTT_15Min) as minRTT_15Min,
    SUM(lostTwampMessages) as lostTwampMessages,
    SUM(txTwampMessages) as txTwampMessages

    from 

    (SELECT *  FROM UMTS_Counters.FTM_TWAMP     
                    
    where 
    DATE_TRUNC ('{time_agg.lower()}',measTime, 'America/Bogota') >='{start_d}' and
    DATE_TRUNC ('{time_agg.lower()}',measTime, 'America/Bogota') <'{end_d_sql}'


    and (
    distName like '{"%%' or distName like '".join(sites_mo)}/%%'
    )

    LIMIT 1 by measTime, measInterval, distName, extendedDistName
    SETTINGS asterisk_include_alias_columns = 1) a

    group by distName, measTime

    order by measTime


    """

    print(str_sql)

    UMTS_TWAMP_Stats = pd.read_sql(str_sql, conn_PM)

    print("TWAMP:" , UMTS_TWAMP_Stats.shape)


    #Convertir a numero los campos

    for counter in UMTS_TWAMP_Stats.columns[2:]:
        try:
            UMTS_TWAMP_Stats[counter]=UMTS_TWAMP_Stats[counter].astype(np.int64)
        except:
            UMTS_TWAMP_Stats[counter]=UMTS_TWAMP_Stats[counter].astype(float)

    UMTS_TWAMP_Stats['distName_p']=UMTS_TWAMP_Stats['distName'].apply(lambda x: x[:x.find('FTM-')-1])


    UMTS_TWAMP_Stats_WBTS=UMTS_TWAMP_Stats.drop('distName', axis=1).groupby(['measTime','distName_p']).sum().reset_index()\
    .merge(BL_WBTS, right_on='mo_distname', left_on='distName_p').drop(['distName_p','mo_distname'], axis=1)


    UMTS_TWAMP_Stats_WBTS['RNC_2627b']=100*(UMTS_TWAMP_Stats_WBTS['txTwampMessages']-UMTS_TWAMP_Stats_WBTS['lostTwampMessages'])/UMTS_TWAMP_Stats_WBTS['txTwampMessages']

    UMTS_TWAMP_Stats_WBTS['M5126C0']=UMTS_TWAMP_Stats_WBTS['avgRTT_15Min']
    UMTS_TWAMP_Stats_WBTS['M5126C1']=UMTS_TWAMP_Stats_WBTS['maxRTT_15Min']

    UMTS_TWAMP_Stats_WBTS=UMTS_TWAMP_Stats_WBTS[['measTime','name','RNC_2627b','M5126C0','M5126C1']]
    UMTS_TWAMP_Stats_WBTS.columns=['Date','Site','RNC_2627b','M5126C0','M5126C1']



    kpis=['RNC_5409a','RNC_1255c','RNC_1254a']

    obj = {
    "tech": "UMTS",
    "kpi_id": ",".join(kpis),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"site_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    try:
        x = requests.post(url, json = obj)
        data = json.loads(x.text)
        DATA_UMTS = pd.DataFrame.from_dict(data)
        print(DATA_UMTS.shape)
    except:
        print("FAIL")

    print("DATA_UMTS:" , DATA_UMTS.shape)


    if DATA_UMTS.shape[0]>0:

        DATA_UMTS=DATA_UMTS.merge(UMTS_TWAMP_Stats_WBTS, left_on=['Date','Site'], right_on=['Date','Site'], how='left')

        BL_WCEL['Site']=BL_WCEL.dropna()['name'].apply(lambda x: x[:x.find("_")])
        BL_WBTS=BL_WCEL[['RNC_name','RNC_id','WBTS_id','Site']].drop_duplicates()

        DATA_UMTS=BL_WBTS.merge(DATA_UMTS, left_on='Site', right_on='Site') 

        DATA_UMTS['Period start time']=DATA_UMTS['Date']
        DATA_UMTS['PLMN name']="PLMN"
        DATA_UMTS['RNC name']=DATA_UMTS['RNC_name']
        DATA_UMTS['WBTS name']=DATA_UMTS['Site']
        DATA_UMTS['WBTS ID']=DATA_UMTS['WBTS_id']



        columns_report=['Period start time','PLMN name','RNC name','WBTS name','WBTS ID']+kpis+['RNC_2627b','M5126C0','M5126C1']

        #Rellenar columnas faltantes

        missing_col=[]

        for column in columns_report:

            if column not in DATA_UMTS.columns:
                missing_col.append(column)

        for column in missing_col:
            DATA_UMTS[column]=np.nan


        #writer = pd.ExcelWriter(f'WBTS_WCDMA17_NPO_MONITORING-RSRAN-WBTS-{time_agg}_{log_control}.xlsx',  engine='xlsxwriter')
        #DATA_UMTS[columns_report].sort_values(by=['WBTS name','Period start time']).to_excel(writer, index=False, sheet_name='data')

        if scheduled:
            print("Llamada por funcion scheduler")
            log_control_sch=datetime.strftime(datetime.today() , '%Y-%m-%d %HH-%MM')
            
        else:
            log_control_sch=log_control
            log_control=datetime.strftime(datetime.today() , '%Y-%m-%d')

        DATA_UMTS[columns_report].sort_values(by=['WBTS name','Period start time']).to_excel(f'WBTS_WCDMA17_NPO_MONITORING-RSRAN-WBTS-{time_agg}_{log_control}.xlsx', index=False, engine='xlsxwriter')

        mem_usage_MB=DATA_UMTS.memory_usage(deep=True).sum()/1000000

        s3.upload_file(f'WBTS_WCDMA17_NPO_MONITORING-RSRAN-WBTS-{time_agg}_{log_control}.xlsx', 'claro-colombia-production' , f'Reports/LTE_700/{log_control}/WBTS_WCDMA17_NPO_MONITORING-RSRAN-WBTS-{time_agg}_{log_control_sch}.xlsx')
        
        import os
        os.remove(f'WBTS_WCDMA17_NPO_MONITORING-RSRAN-WBTS-{time_agg}_{log_control}.xlsx')

    return f"Total Time: {round((time.time()-start_time)/60,2)} minutes", mem_usage_MB







def GSM_BTS_Report(url, conn, conn_PM, s3, WS_dict, start_d, end_d, log_control, time_agg, *scheduled):


    from datetime import date,timedelta, datetime
    end_d_sql=(datetime.strptime(end_d, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
    mem_usage_MB=-1
        
    #print(hist_days, url, WS_dict)

    WS=[WS_dict[i] for i in WS_dict]

    print(WS)

    #API url
    import requests
    import json
    import pandas as pd
    import time

    start_time = time.time()





    #iniciar fechas
    today = date.today()

    #start_d=(today + timedelta(days = -hist_days)).strftime("%Y-%m-%d")

    print(start_d,'\n')




    kpis=['dcr_5','blck_8i','c003038','dlq_2_4','ulq_2_4','uav_15b','ulq_1a','dlq_1a','denied','trf_377','trf_1d','c057017','tbf_15a','tbf_16b','tbf_37d','tbf_38d','trf_233c','trf_234','trf_235b','trf_236','dcr_31b','blck_5a','ava_1g','dis_1','trf_215a','trf_214a','trf_213c','trf_212c','c001033']


    kpis1=kpis[:int(len(kpis)/2)]
    kpis2=kpis[int(len(kpis)/2):]


    kpis_orig=['dcr_5','blck_8i','blck_5a_bh','c003038','dlq_2_4','ulq_2_4','uav_15b','ulq_1a','dlq_1a','denied','trf_377','trf_1d','c057017','tbf_15a','tbf_16b','tbf_37d','tbf_38d','trf_233c','trf_234','trf_235b','trf_236','dcr_31b','blck_5a','ava_1g','ava_68','dis_1','trf_215a','trf_214a','trf_213c','trf_212c','c001033','trf_119']

    print('Ejecutando - 1')

    obj = {
    "tech": "GSM",
    "kpi_id": ",".join(kpis1),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"sector_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    print(obj)

    x = requests.post(url, json = obj)
    data = json.loads(x.text)

    print('Ejecutado GSM p1')
    #print(data)
    
    DATA_GSM = pd.DataFrame.from_dict(data)


    print('Ejecutando - 1')

    obj = {
    "tech": "GSM",
    "kpi_id": ",".join(kpis2),
    "start_date": start_d,
    "end_date": end_d,
    "groupby":"sector_name",
    "site":','.join(WS),
    "timeAgg": time_agg,
    "timeZone": "America/Bogota",
    }

    print(obj)

    x = requests.post(url, json = obj)
    data = json.loads(x.text)

    print('Ejecutado GSM p2')
    #print(data)
    
    DATA_GSM_2 = pd.DataFrame.from_dict(data)

    print(DATA_GSM.info())

    


    if DATA_GSM.shape[0]>0:

        DATA_GSM=DATA_GSM.merge(DATA_GSM_2, left_on=['Date','Cell'], right_on=['Date','Cell'])








        sql_str="""
        select bsc_name as BSC_name, 
        extractAll(moDistName, '([0-9]+)')[1] as BSC_id,
        extractAll(moDistName, '([0-9]+)')[2] as BCF_id,
        extractAll(moDistName, '([0-9]+)')[3] as BTS_id,
        sector_name as name
        from GSM_Site_Database t
        inner join (
        select sector_name, max(export_date) as MaxDate
        from GSM_Site_Database
        group by sector_name
        ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate
        FORMAT TSVWithNamesAndTypes
        """

        BL_BSC = pd.read_sql(sql_str, conn_PM)  

        BL_BSC.drop_duplicates(inplace=True)

        DATA_GSM=BL_BSC.merge(DATA_GSM, left_on='name', right_on='Cell')




        DATA_GSM['Period start time']=DATA_GSM['Date']
        DATA_GSM['PLMN name']="PLMN"
        DATA_GSM['BSC name']=DATA_GSM['BSC_name']
        DATA_GSM['BCF name']=DATA_GSM['Cell'].apply(lambda x: x[:x.find("_")])
        DATA_GSM['BTS name']=DATA_GSM['Cell']

        
        columns_report=['Period start time','BSC name','BCF name','BTS name']+kpis_orig

        #Rellenar columnas faltantes

        missing_col=[]

        for column in columns_report:

            if column not in DATA_GSM.columns:
                missing_col.append(column)

        for column in missing_col:
            DATA_GSM[column]=np.nan
        
        DATA_GSM['ava_68']=np.nan
        DATA_GSM['blck_5a_bh']=DATA_GSM['blck_5a']
        DATA_GSM['trf_119']=DATA_GSM['trf_1d']

        print(DATA_GSM.shape)


        #writer = pd.ExcelWriter(f'GSM_NPO_Monitoring_v4_NOKBSC-BTS-{time_agg}_{log_control}.xlsx',  engine='xlsxwriter')
        #DATA_GSM[columns_report].sort_values(by=['BTS name','Period start time']).to_excel(writer, index=False)

        if scheduled:
            print("Llamada por funcion scheduler")
            log_control_sch=datetime.strftime(datetime.today() , '%Y-%m-%d %HH-%MM')
            
        else:
            log_control_sch=log_control
            log_control=datetime.strftime(datetime.today() , '%Y-%m-%d')            


        DATA_GSM[columns_report].sort_values(by=['BTS name','Period start time']).to_excel(f'GSM_NPO_Monitoring_v4_NOKBSC-BTS-{time_agg}_{log_control}.xlsx', index=False, engine='xlsxwriter')


        mem_usage_MB=DATA_GSM.memory_usage(deep=True).sum()/1000000

        s3.upload_file(f'GSM_NPO_Monitoring_v4_NOKBSC-BTS-{time_agg}_{log_control}.xlsx', 'claro-colombia-production' , f'Reports/LTE_700/{log_control}/GSM_NPO_Monitoring_v4_NOKBSC-BTS-{time_agg}_{log_control_sch}.xlsx')
        
        import os
        os.remove(f'GSM_NPO_Monitoring_v4_NOKBSC-BTS-{time_agg}_{log_control}.xlsx')

    return f"Total Time: {round((time.time()-start_time)/60,2)} minutes", mem_usage_MB







def Run_All_Reports(start_d, end_d, max_agg_hour_days, max_days_sql, time_agg, conn, conn_PM, api_url, s3, sel_sites_ws, log_timestamp):

    print("Starting!!")

    from datetime import date,timedelta, datetime
        
    if ((datetime.strptime(end_d, '%Y-%m-%d')-datetime.strptime(start_d, '%Y-%m-%d')).days >max_agg_hour_days) & (time_agg=='Hour'):

        time_agg='Day'

    if ((datetime.strptime(end_d, '%Y-%m-%d')-datetime.strptime(start_d, '%Y-%m-%d')).days >max_days_sql):

        start_d=(datetime.strptime(end_d, '%Y-%m-%d') + timedelta(days = -max_days_sql)).strftime("%Y-%m-%d")



    #GSM BTS

    print("GSM BTS")
    try:
        elapsed_time, mem_usage=GSM_BTS_Report(api_url, conn, conn_PM, s3, sel_sites_ws, start_d, end_d, log_timestamp, time_agg, True)
        print("DONE!!")

    except Exception as error_rep:
        error_str="At line: " + str(error_rep.__traceback__.tb_lineno) + ": " + type(error_rep).__name__ + "  " + str(error_rep)
        print(error_str)

    print("UMTS WBTS")
    try:
        elapsed_time, mem_usage=UMTS_WBTS_Report(api_url, conn, conn_PM, s3, sel_sites_ws, start_d, end_d, log_timestamp, time_agg, True)
        print("DONE!!")

    except Exception as error_rep:
        error_str="At line: " + str(error_rep.__traceback__.tb_lineno) + ": " + type(error_rep).__name__ + "  " + str(error_rep)
        print(error_str)

    print("UMTS WCEL")
    try:
        elapsed_time, mem_usage=UMTS_WCEL_Report(api_url, conn, conn_PM, s3, sel_sites_ws, start_d, end_d, log_timestamp, time_agg, True)
        print("DONE!!")

    except Exception as error_rep:
        error_str="At line: " + str(error_rep.__traceback__.tb_lineno) + ": " + type(error_rep).__name__ + "  " + str(error_rep)
        print(error_str)

    print("LTE LNCEL")
    try:    
        elapsed_time, mem_usage=LTE_LNCEL_Report(api_url, conn, conn_PM, s3, sel_sites_ws, start_d, end_d, log_timestamp, time_agg, True)
        print("DONE!!")
    except Exception as error_rep:
        error_str="At line: " + str(error_rep.__traceback__.tb_lineno) + ": " + type(error_rep).__name__ + "  " + str(error_rep)
        print(error_str)

    print("LTE LNBTS")
    try:
        elapsed_time, mem_usage=LTE_LNBTS_Report(api_url, conn, conn_PM, s3, sel_sites_ws, start_d, end_d,  log_timestamp, time_agg, True)
        print("DONE!!")
    except Exception as error_rep:
        error_str="At line: " + str(error_rep.__traceback__.tb_lineno) + ": " + type(error_rep).__name__ + "  " + str(error_rep)
        print(error_str)

    return None

def test_hello():
    print('HELLO')
    