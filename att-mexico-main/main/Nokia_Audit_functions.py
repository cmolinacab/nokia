import base64
import io
import os
import os.path, time
import json

import pandasql as psql

import prestodb

from io import StringIO


import plotly.graph_objs as go


from datetime import date,timedelta, datetime
import plotly.express as px

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import dash
from dash import no_update
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from dash import dash_table
import dash_auth



import pandas as pd
import numpy as np
import requests

import math
import itertools
from shapely.geometry import Polygon




from functools import reduce
import operator

import ssl
import warnings

warnings.filterwarnings("ignore")



#Get files from s3

def update_template_vars(s3_bucket,s3):

    from configparser import ConfigParser

    app_path = os.getcwd()
    configur = ConfigParser()   
    configFilePath = f"{app_path}/conf.txt"





    #Read templates from s3

    s3.download_file(s3_bucket, 'templates/Param_Template_Simple_LTE.csv', 'Param_Template_Simple_LTE.csv')
    s3.download_file(s3_bucket, 'templates/Template_Audit.xlsx', 'Template_Audit.xlsx')
    s3.download_file(s3_bucket, 'templates/Template_Audit_KPI.xlsx', 'Template_Audit_KPI.xlsx')
    s3.download_file(s3_bucket, 'templates/conf.txt', configFilePath)



    #Read conf file


    configur.read(configFilePath)


    #Variable de inicio

    LNBTS_version=configur['audit']['LNBTS_version']
    RTWP_Port_Dif=float(configur['audit']['RTWP_Port_Dif'])
    RTWP_Threshold=float(configur['audit']['RTWP_Threshold'])

    RSSI_Th_Outdoor=float(configur['audit']['RSSI_Th_Outdoor'])
    RSSI_Th_Indoor=float(configur['audit']['RSSI_Th_Indoor'])

    Audit_Template=app_path + configur['paths']['Audit_Template']
    Audit_Template_KPI=app_path + configur['paths']['Audit_Template_KPI']

    Logs_Path=app_path + configur['paths']['Logs_Path']

    Share_HO_PCS_RSI=float(configur['audit']['Share_HO_PCS_RSI'])


    #KPI Report

    LTE_KPIS=configur['kpi']['LTE_KPIS']
    KPI_ORDER_REPORT=configur['kpi']['KPI_ORDER_REPORT']

    ##Alarmas

    FM_Exceptions=configur['FM']['FM_Exceptions']
    FM_VSWR=configur['FM']['FM_VSWR']
    FM_PIM=configur['FM']['FM_PIM']

    ##Mail Receivers

    receivers_email=configur['mail']['receivers_email']

    return(LNBTS_version,RTWP_Port_Dif,RTWP_Threshold,RSSI_Th_Outdoor,RSSI_Th_Indoor,Audit_Template,
        Audit_Template_KPI,Logs_Path,Share_HO_PCS_RSI,LTE_KPIS,KPI_ORDER_REPORT,FM_Exceptions,FM_VSWR,FM_PIM,receivers_email)




def send_mail(log_timestamp_dir, log_timestamp, receiver_address, sender_address, Logs_Path, filter_option):

    import smtplib
    import pandas as pd
    from os.path import basename
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    
    #Read Evaluated Sites
    sites=pd.read_csv(f'{Logs_Path}{log_timestamp_dir}/Sites_Names_Log_{log_timestamp}.csv')
    print(str(sites))
    
    files=[]
    
    
    files.append(f'{Logs_Path}{log_timestamp_dir}/Audit_Report_FINAL.xlsx')
    
    if filter_option=='AC':
        

            files.append(f'{Logs_Path}{log_timestamp_dir}/Audit_LNRELW_Detail_Log_{log_timestamp}.csv')


            files.append(f'{Logs_Path}{log_timestamp_dir}/Param_Audit_Detail_Log_{log_timestamp}.csv')


    print(files)



    mail_content = "Sitios Evaluados:\n\n  "+ str(sites)


    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = ", ".join(receiver_address)
    
    message['Subject'] = f'Auditoria Iniciada en: {str(log_timestamp)}'   
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    
    for f in files or []:
        try:
            with open(f, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(f)
                )
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            message.attach(part)  
        except:
            pass
    
    
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('mailrelay.int.nokia.com',25)
    session.connect("mailrelay.int.nokia.com",25)
    session.ehlo()
    context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)    
    context.set_ciphers('DEFAULT@SECLEVEL=1') 
    session.starttls(context=context)
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

    return True




def text_updater(text, path, operation):
    file_update=open(path, operation)
    file_update.write(text)
    file_update.close()
    return file_update


def get_site_list(conn):

    #Presto DB


    query_sites="""select mo_distname, raml_date, element_at(parameters, 'name') as name
                from hive.network_data.cm a,
                (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNBTS') b
                where  a.mo_class = 'LNBTS' and a.raml_date=b.last_date  order by name asc """


    cursor = conn.cursor()

    cursor.execute(query_sites)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    data_CM = pd.DataFrame(data,columns=columns) 

    data_CM.dropna(inplace=True)

    site_list=list(data_CM[['name','mo_distname']].itertuples(index=False, name=None))

    return site_list





#def Audit_SW_Load_Version(version, modist_sites, path_log_dir, host, port, user, catalog, schema):

def Audit_SW_Load_Version(version, modist_sites, path_log_dir, conn):


    sql_str="""
    select element_at(ids,'MRBTS') as MRBTS_id, 
    mo_distname, mo_version, raml_date,
    element_at(parameters, 'name') as LNBTS_Name,
    element_at(parameters, 'enbName') as enbName,
    element_at(parameters, 'operationalState') as operationalState

    from hive.network_data.cm a,

    (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNBTS') b

    where
    mo_class = 'LNBTS' and a.raml_date=b.last_date
    
    and ("""

    query_sites=''
    
    for site in modist_sites:

        query_sites=query_sites + f"mo_distname = '{site}' or "
        
    sql_str=sql_str + query_sites[:-4]+")"
    

    cursor = conn.cursor()

    cursor.execute(sql_str)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    data_CM = pd.DataFrame(data,columns=columns)
    
    data_CM['Version_OK']=np.where(data_CM['mo_version']==version,"OK","NO-OK")

    result=data_CM[['LNBTS_Name','mo_distname','mo_version','Version_OK']]

    result.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/LNBTS_HW_Audit_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    
    return result







def Param_Template_Audit(Template_ALL, modist_sites, path_log_dir, conn):

    path_text_update=path_log_dir

    start_time = time.time()

    param_dev_ALL=pd.DataFrame()
    param_count=pd.DataFrame()




    for Table in set(Template_ALL['Object']):
        parameters=set(Template_ALL[Template_ALL['Object']==Table]['Parameter'])

        param_sql='select mo_distname, raml_date, '

        for parameter in parameters:
            param_sql=param_sql + "element_at(parameters, '" + parameter+ "') as "+ parameter +", "

        param_sql = param_sql[:-2] + f" from hive.network_data.cm a,"
        param_sql = param_sql + f" (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = '{Table}') b"
        param_sql = param_sql + f" where  a.mo_class = '{Table}' and a.raml_date=b.last_date"

        #Itearte over sites list
        query_sites=''

        for site in modist_sites:

            query_sites=query_sites + f"mo_distname like '{site}%' or "

        param_sql = param_sql + f" and ({query_sites[:-4]})"

        #print(param_sql)

        cursor = conn.cursor()

        #print(f"Querying Table: {Table}")

        write_status=text_updater(f"**Querying Table:** {Table}", path_text_update, 'a')

        cursor.execute(param_sql)

        data = cursor.fetchall()
        columns = [c[0] for c in cursor.description]

        data_CM = pd.DataFrame(data,columns=columns)

        #print(data_CM.shape)

        cursor.close() 

        #Compare values

        param_compare=data_CM.melt(id_vars=['mo_distname','raml_date'], 
                               var_name='Parameter', value_name='Cur_Value').merge(Template_ALL[Template_ALL['Object']==Table],
                                                           left_on='Parameter', right_on='Parameter')


        #Contabilizar total de elementos por parametro como denominador del calculo de desviacion
        param_count=pd.concat([param_count,
                              param_compare.groupby(['mo_distname','Object','Parameter']).count().reset_index()\
                              [['mo_distname','Object','Parameter','Value']]])

        #Parametros desviados
        param_dev_table=param_compare[param_compare['Cur_Value']!=param_compare['Value']]


        if param_compare.shape[0]>0:

            #print(f"Desviados:  {param_dev_table.shape[0]} de {param_compare.shape[0]} -> {round(100*param_dev_table.shape[0]/param_compare.shape[0],2)}%\n")
            write_status=text_updater(f"  ->  **Desviados:**  {param_dev_table.shape[0]} de {param_compare.shape[0]} -> {round(100*param_dev_table.shape[0]/param_compare.shape[0],2)}%\n", path_text_update, 'a')
        else:
            #print(f"Tabla {Table} vacia\n")
            write_status=text_updater(f"  ->  **Tabla {Table} vacia**\n", path_text_update, 'a')

        param_dev_ALL=pd.concat([param_dev_ALL, param_dev_table])
        
    #print("Total Time:",(time.time()-start_time)/60, "minutes")  
    #write_status=text_updater("Total Time: "+str((time.time()-start_time)/60) + " minutes\n", path_text_update, 'a') 
    


    
    param_count['site_id']=""
    param_dev_ALL['site_id']=""

    for site in modist_sites:

        param_count['site_id'] = param_count.apply(lambda row : \
                                                             site if site in row['mo_distname'] else row['site_id'], axis = 1)

        param_dev_ALL['site_id'] = param_dev_ALL.apply(lambda row : \
                                                             site if site in row['mo_distname'] else row['site_id'], axis = 1)

    
    
    #summary=param_count[['site_id','mo_distname']].groupby('site_id').count().reset_index().merge(\
    #                                param_dev_ALL[['site_id','mo_distname']].groupby('site_id').count().reset_index(),
    #                                left_on='site_id', right_on='site_id', suffixes=['_total','_dev'])
    
    summary=param_count[['site_id','Object','mo_distname']].groupby(['site_id','Object']).count().reset_index().merge(\
                                param_dev_ALL[['site_id','Object','mo_distname']].groupby(['site_id','Object']).count().reset_index(),
                                left_on=['site_id','Object'], right_on=['site_id','Object'], suffixes=['_total','_dev'])


    summary['Deviation']=round(100*summary['mo_distname_dev']/summary['mo_distname_total'],2)

    summary.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Param_Audit_Summary_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    param_dev_ALL.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Param_Audit_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:], index=False)
    
    return summary, param_dev_ALL



def PCI_RSI_Audit(modist_sites, path_log_dir, conn, conn_PM, Share_HO_PCS_RSI):


    import pyproj

    geodesic = pyproj.Geod(ellps='WGS84')

    start_time = time.time()


    # In[9]:


    #Estadisticas de HO por adj

    sql_str=f"""
    select    

    x.sector_name,
    distName,
    substring (extendedDistName, position(extendedDistName, 'ECI')+4, position(extendedDistName, 'PLMN', position(extendedDistName, 'ECI'))-position(extendedDistName, 'ECI')-5) as ECI,

    sum(M8015C8) as INTER_HO_ATT_NB,
    sum(M8015C9) as INTER_HO_SUCC_NB
    from LTE_Counters.LTE_Neighb_Cell_HO a FINAL,



        (select *
        from LTE_Site_Database t
        inner join (
        select sector_name, max(export_date) as MaxDate
        from LTE_Site_Database
        group by sector_name
        ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate ) x
                    
                    
    where 
    a.distName=x.moDistName 

    and measTime>toDate(now())-3 and M8015C8>0

    and ("""


    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"a.distName like '{site}%%' or "

    sql_str=sql_str + query_sites[:-4]+") group by sector_name, distName, ECI "


    # In[10]:


    Data_HO = pd.read_sql(sql_str, conn_PM)

    print("Data_HO:",Data_HO.shape)


    # In[11]:


    Data_HO['INTER_HO_ATT_NB']=Data_HO['INTER_HO_ATT_NB'].astype(int)
    Data_HO['INTER_HO_SUCC_NB']=Data_HO['INTER_HO_SUCC_NB'].astype(int)


    # In[12]:


    #Parámetros de LNCEL

        
    sql_str=f"""

    select 
    a.LNCEL_id,
    a.LNBTS_id,
    a.mo_distname, 
    LNCEL_Name, 
    phyCellId, 
    eutraCelId,
    earfcnDL,
    rootSeqIndex,
    prachCS,
    prachFreqOff,
    prachConfIndex

    from

    (
    select 
    element_at(ids,'LNCEL') as LNCEL_id,
    element_at(ids,'LNBTS') as LNBTS_id,
    mo_distname,
    element_at(parameters, 'name') as LNCEL_Name,
    element_at(parameters, 'phyCellId') as phyCellId,
    element_at(parameters, 'eutraCelId') as eutraCelId

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNCEL') b

    where
    mo_class = 'LNCEL' and a.raml_date=b.last_date

    ) a,

    (
    select 
    element_at(ids,'LNCEL') as LNCEL_id,
    element_at(ids,'LNBTS') as LNBTS_id,
    mo_distname,
    element_at(parameters, 'earfcnDL') as earfcnDL,
    element_at(parameters, 'rootSeqIndex') as rootSeqIndex,
    element_at(parameters, 'prachCS') as prachCS,
    element_at(parameters, 'prachFreqOff') as prachFreqOff,
    element_at(parameters, 'prachConfIndex') as prachConfIndex

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNCEL_FDD') b

    where
    mo_class = 'LNCEL_FDD' and a.raml_date=b.last_date

    ) b

    where a.LNCEL_id=b.LNCEL_id and a.LNBTS_id=b.LNBTS_id
    """


    # In[13]:


    cursor = conn.cursor()

    cursor.execute(sql_str)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    LNCEL = pd.DataFrame(data,columns=columns)


    # In[14]:


    #Parámetros de LNADJ


    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"a.mo_distname like '{site}%%' or "
        
        
    sql_str=f"""

    select 
    element_at(ids,'LNBTS') as LNBTS_id,
    element_at(parameters, 'adjEnbId') as adjEnbId

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNADJ') b

    where
    mo_class = 'LNADJ' and a.raml_date=b.last_date
    and ({query_sites[:-4]})
    """


    # In[15]:


    cursor = conn.cursor()

    cursor.execute(sql_str)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    LNADJ = pd.DataFrame(data,columns=columns)


    # In[16]:


    ## BAseline 4G


    start_date=datetime.strftime(datetime.today() , "%Y-%m-%d")

    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"moDistName like '{site}%%' or "
        
        

    str_sql=f"""select export_date, moDistName, sector_name, site_name, latitud, longitud
                    from LTE_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from LTE_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """


    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_4G = pd.read_sql(str_sql, conn_PM)


    # ##  Link de tablas

    # In[17]:


    Share=Data_HO[['sector_name','INTER_HO_ATT_NB']].groupby('sector_name').sum().reset_index()


    # In[18]:


    #Query to build formula

    query="""

    select
    Data_HO.sector_name as LNCEL_Name, LNCEL.LNCEL_Name as LNCEL_Name_tgt, 
    Data_HO.INTER_HO_ATT_NB,  Share.INTER_HO_ATT_NB as Total_ATT , 100*Data_HO.INTER_HO_ATT_NB/Share.INTER_HO_ATT_NB as Share   
    from 
    Data_HO, LNCEL, Share
    where Data_HO.ECI=LNCEL.eutraCelId and Data_HO.sector_name=Share.sector_name

                
    """

    df_HO = psql.sqldf(query, locals())


    # In[19]:


    Baseline_Param2=LNCEL.merge(Tabla_Ref_4G,left_on='mo_distname', right_on='moDistName')
    Baseline_Param2.drop(['export_date','moDistName','sector_name'], axis=1, inplace=True)


    # ### PCI Confilcts

    # In[20]:


    PCI_Conflicts=Baseline_Param2.merge(LNADJ, left_on='LNBTS_id', right_on='LNBTS_id').merge(Baseline_Param2, left_on=['adjEnbId','earfcnDL','phyCellId'], 
                          right_on=['LNBTS_id','earfcnDL','phyCellId'], suffixes=['_Orig','_Dest']).\
                    merge(df_HO,left_on=['LNCEL_Name_Orig','LNCEL_Name_Dest'],right_on=['LNCEL_Name','LNCEL_Name_tgt'])


    # In[21]:


    if PCI_Conflicts.shape[0]>0:
        PCI_Conflicts['Distance']= PCI_Conflicts.apply(lambda row: geodesic.inv(row['longitud_Orig'],row['latitud_Orig'], 
                                        row['longitud_Dest'],row['latitud_Dest'])[2], axis=1) #DISTANCIA
        
        PCI_Conflicts.drop(['phyCellId_Orig','eutraCelId_Orig','latitud_Orig','longitud_Orig','adjEnbId','mo_distname_Dest',
                       'phyCellId_Dest','eutraCelId_Dest','site_name_Dest','latitud_Dest','longitud_Dest'],
                      axis=1, inplace=True)
        
    
    #write_status=text_updater('Confilctos PCI:'+str(PCI_Conflicts.shape[0])+"\n", path_log_dir, 'a')  
    #write_status=text_updater('Confilctos PCI Share HO:'+str(PCI_Conflicts[PCI_Conflicts['Share']>0].shape[0])+"\n", path_log_dir, 'a') 
    #print('Confilctos PCI:',PCI_Conflicts.shape[0])
    #print('Confilctos PCI Share HO:',PCI_Conflicts[PCI_Conflicts['Share']>0].shape[0])


    # ### RSI Conflicts

    # In[22]:


    RSI_Conflicts=Baseline_Param2.merge(LNADJ, left_on='LNBTS_id', right_on='LNBTS_id').                merge(Baseline_Param2, left_on=['adjEnbId','earfcnDL','rootSeqIndex'], 
                          right_on=['LNBTS_id','earfcnDL','rootSeqIndex'], suffixes=['_Orig','_Dest']).\
                    merge(df_HO,left_on=['LNCEL_Name_Orig','LNCEL_Name_Dest'],right_on=['LNCEL_Name','LNCEL_Name_tgt'])


    # In[23]:


    if RSI_Conflicts.shape[0]>0:
        RSI_Conflicts['Distance']= RSI_Conflicts.apply(lambda row: geodesic.inv(row['longitud_Orig'],row['latitud_Orig'], 
                                        row['longitud_Dest'],row['latitud_Dest'])[2], axis=1) #DISTANCIA
        RSI_Conflicts.drop(['phyCellId_Orig','eutraCelId_Orig','latitud_Orig','longitud_Orig','adjEnbId','mo_distname_Dest',
                       'phyCellId_Dest','eutraCelId_Dest','site_name_Dest','latitud_Dest','longitud_Dest',
                           'LNCEL_Name', 'LNCEL_Name_tgt'],
                      axis=1, inplace=True)


    PCI_Conflicts=PCI_Conflicts[PCI_Conflicts['Share']>=Share_HO_PCS_RSI][['site_name_Orig','LNCEL_Name_Orig','LNCEL_Name_Dest','phyCellId','Share']]

    RSI_Conflicts=RSI_Conflicts[RSI_Conflicts['Share']>=Share_HO_PCS_RSI][['site_name_Orig','LNCEL_Name_Orig','LNCEL_Name_Dest','rootSeqIndex','Share']]

    write_status=text_updater('**Conflictos PCI:**\n'+PCI_Conflicts.to_string(index=False)+"\n", path_log_dir, 'a')
    write_status=text_updater('**Conflictos RSI:**\n'+RSI_Conflicts.to_string(index=False)+"\n", path_log_dir, 'a')


    PCI_Conflicts.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/PCI_Conflicts_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    RSI_Conflicts.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/RSI_Conflicts_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:])


    #print("Total Time:",(time.time()-start_time)/60, "minutes")

    return PCI_Conflicts, RSI_Conflicts


def RSSI_Audit(modist_sites, path_log_dir, conn_PM, RSSI_Th_Outdoor, RSSI_Th_Indoor, api_url):

    start_time = time.time()

    start_date=datetime.strftime(datetime.today() , "%Y-%m-%d")

    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, longitud, latitud, site_type
                    from LTE_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from LTE_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """


    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_4G = pd.read_sql(str_sql, conn_PM)

    #print("Tabla_Ref_4G:",Tabla_Ref_4G.shape)




    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, longitud, latitud, site_type
                    from UMTS_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from UMTS_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """


    #Ejecución del QUERY , se guarde la data en DATA 
    #Tabla_Ref_3G = pd.read_sql(str_sql, conn_PM)

    Tabla_Ref_4G['mo_distname_p']=Tabla_Ref_4G['mo_distname'].apply(lambda x: x[:x.find('LNCEL')-1])
    #Tabla_Ref_3G['mo_distname_p']=Tabla_Ref_3G['mo_distname'].apply(lambda x: x[:x.find('LNCEL')-1])

    Sites_Eval=list(set(Tabla_Ref_4G[Tabla_Ref_4G['mo_distname_p'].isin(modist_sites)]['site_name']))

    start_date=(datetime.now()- timedelta(hours = 48)).strftime("%Y-%m-%d")


    # Call to Nokia KPI API
    import requests
    import json





    obj = {
        "tech": "LTE",
        "kpi_id": "LTE_5441b,LTE_5444b",
        "start_date": start_date,
        "timeAgg": "Hour",
        "groupby": "sector_name",
        "site": ",".join(Sites_Eval),
        "timeZone": "America/Mexico_City"
    }

    #print(obj)


    x = requests.post(api_url, json = obj)
    data = json.loads(x.text)

    DATA_RSSI = pd.DataFrame.from_dict(data)

    #print("DATA_RSSI:",DATA_RSSI.shape,obj)

    Avg_RSSI=DATA_RSSI.drop('Date',axis=1).groupby('Cell').min().reset_index()


    Avg_RSSI.columns=['Cell','Avg RSSI for PUCCH', 'Avg RSSI for PUSCH']

    Avg_RSSI=Avg_RSSI.merge(Tabla_Ref_4G[['sector_name','site_type']], left_on='Cell', right_on='sector_name')
    
    Avg_RSSI['Threshold']=np.where(Avg_RSSI['site_type']=='Indoor',RSSI_Th_Indoor,RSSI_Th_Outdoor)

    #Audit_RSSI_Th=Avg_RSSI[(Avg_RSSI['Avg RSSI for PUCCH']>Avg_RSSI['Threshold']) | (Avg_RSSI['Avg RSSI for PUSCH']>Avg_RSSI['Threshold'])]\
    #.drop('sector_name', axis=1)

    Audit_RSSI_Th=Avg_RSSI[Avg_RSSI['Avg RSSI for PUSCH']>Avg_RSSI['Threshold']]\
    .drop('sector_name', axis=1)

    write_status=text_updater('\n**RSSI Mayor al Umbral:**\n'+Audit_RSSI_Th.to_string(index=False)+"\n", path_log_dir, 'a')


    Audit_RSSI_Th.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_RSSI_Th_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:])


    return Audit_RSSI_Th




def RTWP_Audit(modist_sites, path_log_dir, conn_PM, diff_rtwp, rtwp_th):

    #Estadisticas de HO por adj

    sql_str=f"""
    select    

    x.sector_name,
    distName,

    round(avg(M8005C306)/10,2) as AVG_RTWP_RX_ANT_1,
    round(avg(M8005C307)/10,2) as AVG_RTWP_RX_ANT_2,
    round(avg(M8005C308)/10,2) as AVG_RTWP_RX_ANT_3,
    round(avg(M8005C309)/10,2) as AVG_RTWP_RX_ANT_4

    from LTE_Counters.LTE_Pwr_and_Qual_UL a FINAL,



        (select *
        from LTE_Site_Database t
        inner join (
        select sector_name, max(export_date) as MaxDate
        from LTE_Site_Database
        group by sector_name
        ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate ) x
                    
                    
    where 
    a.distName=x.moDistName 

    and measTime>toDate(now())-1 

    and ("""


    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"a.distName like '{site}%%' or "

    sql_str=sql_str + query_sites[:-4]+") group by sector_name, distName "

    Data_RTWP = pd.read_sql(sql_str, conn_PM)

    #print("Data_RTWP:",Data_RTWP.shape, sql_str)

    Data_RTWP.fillna(-120, inplace=True)


    def rtwp_dif(list_rtwp):
        list_rtwp=[i for i in list_rtwp if i]
        return abs(min(list_rtwp)-max(list_rtwp)), sum(list_rtwp)/len(list_rtwp)

    Data_RTWP=pd.concat([Data_RTWP, Data_RTWP.apply(lambda row: rtwp_dif(list(map(float,row[2:6].values))), axis=1, result_type='expand')], axis='columns')
    Data_RTWP.rename(columns={0:'Max_RTWP_Diff', 1:'Avg_RTWP'}, inplace=True)

    #Data_RTWP['Max_RTWP_Diff']=Data_RTWP.apply(lambda row: rtwp_dif(list(map(float,row[2:6].values))), axis=1)

    Audit_RTWP_Diff=Data_RTWP[Data_RTWP['Max_RTWP_Diff']>=diff_rtwp]
    Audit_RTWP_Th=Data_RTWP[Data_RTWP['Avg_RTWP']>=rtwp_th]


    write_status=text_updater('\n**Diferencia de Puertos:**\n'+Audit_RTWP_Diff.to_string(index=False)+"\n", path_log_dir, 'a')
    write_status=text_updater('\n**RTWP Mayor Umbral:**\n'+Audit_RTWP_Th.to_string(index=False)+"\n", path_log_dir, 'a')


    Audit_RTWP_Diff.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_RTWP_Diff_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    Audit_RTWP_Th.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_RTWP_Th_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    

    return Audit_RTWP_Diff, Audit_RTWP_Th





#_________________--------


def TAC_LAC_Audit(modist_sites, path_log_dir, conn, conn_PM):


    start_time = time.time()

    start_date=datetime.strftime(datetime.today() , "%Y-%m-%d")

    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, longitud, latitud
                    from LTE_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from LTE_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """


    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_4G = pd.read_sql(str_sql, conn_PM)




    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, longitud, latitud
                    from UMTS_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from UMTS_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """


    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_3G = pd.read_sql(str_sql, conn_PM)

    Tabla_Ref_4G['mo_distname_p']=Tabla_Ref_4G['mo_distname'].apply(lambda x: x[:x.find('LNCEL')-1])
    Tabla_Ref_3G['mo_distname_p']=Tabla_Ref_3G['mo_distname'].apply(lambda x: x[:x.find('LNCEL')-1])

###

#Site names on evaluation

    Sites_Eval=list(set(Tabla_Ref_4G[Tabla_Ref_4G['mo_distname_p'].isin(modist_sites)]['site_name']))


    #3G LAC

    sql_str="""
    select 
    mo_distname,
    element_at(parameters, 'name') as sector_name,
    element_at(parameters, 'LAC') as LAC

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'WCEL') b

    where
    mo_class = 'WCEL' and a.raml_date=b.last_date
    """

    cursor = conn.cursor()

    cursor.execute(sql_str)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    WCEL_LAC = pd.DataFrame(data,columns=columns)


    # LTE TAC

    sql_str="""
    select 
    mo_distname,
    element_at(parameters, 'name') as sector_name,
    element_at(parameters, 'tac') as TAC

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNCEL') b

    where
    mo_class = 'LNCEL' and a.raml_date=b.last_date
    """

    cursor = conn.cursor()

    cursor.execute(sql_str)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    LNCEL_TAC = pd.DataFrame(data,columns=columns)

#_____-_------

    Tabla_Ref_4G=Tabla_Ref_4G.merge(LNCEL_TAC, left_on=['mo_distname','sector_name'], right_on=['mo_distname','sector_name'])
    Tabla_Ref_3G=Tabla_Ref_3G.merge(WCEL_LAC, left_on=['mo_distname','sector_name'], right_on=['mo_distname','sector_name'])

    ntac=Tabla_Ref_4G[Tabla_Ref_4G['mo_distname_p'].isin(modist_sites)][['site_name','TAC']].groupby(['site_name']).agg(['count', 'nunique']).reset_index()

    #Mas de un LAC en el mismo sitio

    #mult_lac=nlac[nlac['LAC','nunique']>1]

    #Mas de un LAC en el mismo sitio

    mult_tac=ntac[ntac['TAC','nunique']>1]

    if mult_tac.shape[0]>0:
        write_status=text_updater('\n**Multi TAC Configurado:**\n'+mult_tac.to_string(index=False)+"\n", path_log_dir, 'a')


    TAC_LAC_Network=Tabla_Ref_4G[['site_name','longitud', 'latitud', 'TAC']].drop_duplicates().merge(Tabla_Ref_3G[['site_name','LAC']].drop_duplicates(),
                                                                  left_on='site_name', right_on='site_name')
    TAC_LAC_Combinations=TAC_LAC_Network[~TAC_LAC_Network['site_name'].isin(Sites_Eval)][['TAC','LAC']].drop_duplicates()
    TAC_LAC_Combinations['Status']='OK'


    TAC_LAC_Sites_Eval=TAC_LAC_Network[TAC_LAC_Network['site_name'].isin(Sites_Eval)]\
                                [['site_name','TAC','LAC']].drop_duplicates()

    TAC_LAC_Final=TAC_LAC_Sites_Eval.merge(TAC_LAC_Combinations, left_on=['TAC','LAC'], right_on=['TAC','LAC'], how="left").fillna('NO-OK')

    TAC_LAC_Final=TAC_LAC_Final.merge(Tabla_Ref_4G[['site_name','longitud','latitud']].drop_duplicates(), left_on=['site_name'], right_on=['site_name'])


    Audit_Result=TAC_LAC_Final[['site_name','TAC','LAC','Status']]
    write_status=text_updater('\n'+Audit_Result.to_string(index=False)+"\n", path_log_dir, 'a')

    # generar Mapas

    write_status=text_updater('\n**Generando Mapas por Sitio...**\n', path_log_dir, 'a')


    for index, row in TAC_LAC_Final.iterrows():
        
        TAC_LAC_Network_Paint=TAC_LAC_Network[~TAC_LAC_Network['site_name'].isin(Sites_Eval)].copy()
        TAC_LAC_Network_Paint['Marker']=2
        TAC_LAC_Network_Paint=TAC_LAC_Network_Paint.dropna()
        
        
        Cur_site = {'site_name': row['site_name'], 'longitud': row['longitud'], 'latitud': row['latitud'],
                   'TAC': row['TAC'],'LAC': row['LAC'], 'Marker':8}
        TAC_LAC_Network_Paint = TAC_LAC_Network_Paint.append(Cur_site, ignore_index = True)
        
        #print(Cur_site['site_name'])
        write_status=text_updater(f"\nGenerando **{Cur_site['site_name']}** ...\n", path_log_dir, 'a')
        
        TAC_LAC_Network_Paint['TAC_LAC']=TAC_LAC_Network_Paint['TAC']+'_'+TAC_LAC_Network_Paint['LAC']

        print('p1')
        
        fig = px.scatter_mapbox(TAC_LAC_Network_Paint, lat="latitud", lon="longitud", hover_name="site_name", hover_data=["TAC_LAC"],
                                color="TAC_LAC", zoom=12, height=600,size= 'Marker',  
                                title=row['site_name']+'   TAC LAC '+row['TAC']+' '+row['LAC'],
        center={"lat":Cur_site['latitud'], "lon":Cur_site['longitud']})

        print('p2')

        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(showlegend=False)

        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})

        print('p2.5')

        
        fig.to_image(format="png", engine="kaleido")

        print('p3')
        #fig.write_image(f"D:/ClickHouse_NOKIA/{Cur_site['site_name']}.png")

        fig.write_image(path_log_dir[:path_log_dir.rfind('/')]+f"/{Cur_site['site_name']}.png")

        print('p4')


    return Audit_Result


def LNRELW_Audit(modist_sites, path_log_dir, conn, conn_PM):


    start_date=datetime.strftime(datetime.today() , "%Y-%m-%d")

    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, azimuth
                    from LTE_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from LTE_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    where ("""


    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"moDistName like '{site}%%' or "

    str_sql=str_sql + query_sites[:-4]+") "
    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_4G = pd.read_sql(str_sql, conn_PM)




    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, azimuth
                    from UMTS_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from UMTS_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """


    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_3G = pd.read_sql(str_sql, conn_PM)

    Tabla_Ref_4G['mo_distname_p']=Tabla_Ref_4G['mo_distname'].apply(lambda x: x[:x.find('LNCEL')-1])

    LNRELW_Table_Ref=Tabla_Ref_4G.merge(Tabla_Ref_3G, left_on='site_name', right_on='site_name', suffixes=['_src','_tgt'])
    LNRELW_Table_Ref['dif_az']=abs((LNRELW_Table_Ref['azimuth_src']-LNRELW_Table_Ref['azimuth_tgt']+180)%360-180)
    LNRELW_Table_Ref=LNRELW_Table_Ref[LNRELW_Table_Ref['dif_az']<=20].copy()


    str_sql="""select 
    element_at(ids,'LNCEL') as WCEL_id,
    element_at(ids,'LNBTS') as WBTS_id,
    mo_distname,
    element_at(parameters, 'uTargetCid') as uTargetCid,
    element_at(parameters, 'uTargetRncId') as uTargetRncId,
    element_at(parameters, 'targetCellDn') as targetCellDn

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNRELW') b

    where
    mo_class = 'LNRELW' and a.raml_date=b.last_date and ("""


    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"mo_distname like '{site}%%' or "

    str_sql=str_sql + query_sites[:-4]+")"



    cursor = conn.cursor()

    cursor.execute(str_sql)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    LNRELW = pd.DataFrame(data,columns=columns)

    LNRELW['mo_distname_p']=LNRELW['mo_distname'].apply(lambda x: x[:x.find('LNRELW')-1])


    LNRELW_Join=LNRELW_Table_Ref.merge(LNRELW[['mo_distname_p','targetCellDn']], left_on=['mo_distname_src','mo_distname_tgt'],
    right_on=['mo_distname_p','targetCellDn'],how='left')


    LNRELW_Missing=LNRELW_Join[LNRELW_Join['targetCellDn'].isnull()].drop(['mo_distname_p_y',
                                                            'targetCellDn','mo_distname_p_x'], axis=1)

    summary=LNRELW_Missing[['site_name','sector_name_tgt']].groupby('site_name').count().reset_index()


    write_status=text_updater('\n**LNRELW Faltantes:**\n'+summary.to_string(index=False)+"\n", path_log_dir, 'a')

    summary.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_LNRELW_Summary_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    LNRELW_Missing.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_LNRELW_Detail_"+path_log_dir[path_log_dir.rfind('/'):][7:], index=False)


    return summary





def RET_Audit(modist_sites, path_log_dir, conn, conn_PM):


    start_date=datetime.strftime(datetime.today() , "%Y-%m-%d")

    str_sql=f"""select site_name, sector_name, electrical_tilt
                    from LTE_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from LTE_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    where ("""


    query_sites=''

    for site in modist_sites:

        query_sites=query_sites + f"moDistName like '{site}%%' or "

    str_sql=str_sql + query_sites[:-4]+") "

    Tabla_Ref_4G = pd.read_sql(str_sql, conn_PM)



    #Data CM

    query_sites='('

    for site in modist_sites:

        query_sites=query_sites + f"mo_distname like '{site}%%' or "

    query_sites=query_sites[:-4]+")"


    str_sql=f"""

     
    select distinct l.name, l.mo_distname, antlDN, angle as RET_electrical_tilt

    from

    (select 
    element_at(ids,'MRBTS') as MRBTS_id,
    element_at(ids,'LCELL') as LCELL_id,
    element_at(ids,'LCELW') as LCELW_id,
    element_at(ids,'LCELNR') as LCELNR_id,
    element_at(ids,'LCELC') as LCELC_id,
    element_at(parameters, 'antlDN') as antlDN,
    element_at(parameters, 'resourceDN') as resourceDN

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'CHANNEL') b

    where
    mo_class = 'CHANNEL' and a.raml_date=b.last_date and element_at(ids,'LCELL') is not null) c,

    -- LNCEL

    (select 
    element_at(ids,'MRBTS') as MRBTS_id,
    element_at(ids,'LNBTS') as LNBTS_id,
    element_at(ids,'LNCEL') as LNCEL_id,
    mo_distname,
    element_at(parameters, 'lcrId') as lcrId,
    element_at(parameters, 'name') as name

    from hive.network_data.cm a,
     (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNCEL') b

    where
    mo_class = 'LNCEL' and a.raml_date=b.last_date 
    and ({query_sites})

    ) l,

    (
    select 
    element_at(ids,'MRBTS') as MRBTS_id,
    element_at(ids,'EQM') as EQM_id,
    element_at(ids,'APEQM') as APEQM_id,
    element_at(ids,'ALD') as ALD_id,
    mo_distname,
    element_at(parameters, 'sectorID') as sectorID,
    element_at(parameters, 'angle') as angle,
    element_at(parameters, 'antModel') as antModel,
    element_at(parameters, 'antSerial') as antSerial,
    element_at(parameters, 'maxAngle') as maxAngle,
    element_at(parameters, 'mechanicalAngle') as mechanicalAngle,
    element_at(parameters, 'minAngle') as minAngle,
    element_at(parameters, 'subunitNumber') as subunitNumber,
    element_at(parameters, 'baseStationID') as baseStationID,
    antlDNList

    from hive.network_data.cm a 
    CROSS JOIN UNNEST(element_at(p_lists, 'antlDNList')) AS t (antlDNList)
    CROSS join (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'ALD') b
     

    where
    mo_class = 'RETU' and a.raml_date=last_date and element_at(p_lists, 'antlDNList') is not null
    ) r



    where

    c.MRBTS_id = l.MRBTS_id AND c.LCELL_id = l.lcrId

    and r.MRBTS_id=l.MRBTS_id and c.antlDN=r.antlDNList


    order by name, antlDN
    """



    cursor = conn.cursor()

    cursor.execute(str_sql)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    RET = pd.DataFrame(data,columns=columns)

    RET['RET_electrical_tilt']=RET['RET_electrical_tilt'].astype(int)/10

    RET_Table_Ref=RET.merge(Tabla_Ref_4G, left_on='name', right_on='sector_name')


    #Diferencia RET y Baseline
    Summary_Tilt_Dif=RET_Table_Ref[RET_Table_Ref['RET_electrical_tilt']!=RET_Table_Ref['electrical_tilt']]\
                            [['site_name','sector_name','mo_distname','RET_electrical_tilt','electrical_tilt']].drop_duplicates()



    #Diferencias entre RET del mismo sector
    RET_Angles=RET.groupby(['name'])['RET_electrical_tilt'].apply(list).reset_index(name='Angles')

    RET_Angles['RET_Audit']=RET_Angles['Angles'].apply(lambda x: len(set(x)))

    Summary_Same_Sector=RET_Angles[RET_Angles['RET_Audit']>1][['name','Angles']]

    Summary_Same_Sector=Summary_Same_Sector.merge(Tabla_Ref_4G, left_on='name', right_on='sector_name')[['sector_name','Angles','electrical_tilt']]



    write_status=text_updater('\n**RET Dispares mismo Sector:**\n'+Summary_Same_Sector.to_string(index=False)+"\n", path_log_dir, 'a')
    
    write_status=text_updater('\n**RET Diferentes al Baseline:**\n'+Summary_Tilt_Dif.to_string(index=False)+"\n", path_log_dir, 'a')


    Summary_Same_Sector.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_RET_Same_Sector_"+path_log_dir[path_log_dir.rfind('/'):][7:])
    Summary_Tilt_Dif.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Audit_RET_Baseline_Diff_"+path_log_dir[path_log_dir.rfind('/'):][7:])

    return Summary_Same_Sector



def LTE_KPIS_Hist(modist_sites, path_log_dir, conn_PM, KPIS_LTE, Api_URL, KPI_ORDER_REPORT):

    start_time = time.time()

    start_date=datetime.strftime(datetime.today() , "%Y-%m-%d")

    

    str_sql=f"""select moDistName as mo_distname, sector_name, site_name, longitud, latitud
                    from LTE_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from LTE_Site_Database
                    where export_date<=toDateTime('{start_date}')
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 
                    """

    #print(str_sql)
    #Ejecución del QUERY , se guarde la data en DATA 
    Tabla_Ref_4G = pd.read_sql(str_sql, conn_PM)

    Tabla_Ref_4G['mo_distname_p']=Tabla_Ref_4G['mo_distname'].apply(lambda x: x[:x.find('LNCEL')-1])

    Sites_Eval=list(set(Tabla_Ref_4G[Tabla_Ref_4G['mo_distname_p'].isin(modist_sites)]['site_name']))

    Sites_Eval_Out=Tabla_Ref_4G[Tabla_Ref_4G['mo_distname_p'].isin(modist_sites)][['mo_distname_p','site_name']].drop_duplicates().copy()

    #10 dias de historia

    m_date_check=(datetime.now()- timedelta(hours = 240*2)).strftime("%Y-%m-%d")
    m_date_end=(datetime.now()- timedelta(hours = 24)).strftime("%Y-%m-%d")

    #print(Tabla_Ref_4G.shape)



    obj = {
        "tech": "LTE",
        "kpi_id": (",").join(KPIS_LTE.split(",")),
        "start_date": m_date_check,
        "end_date": m_date_end,
        "site": ",".join(Sites_Eval),
        "timeAgg": "Day",
        "timeZone": "America/Mexico_City"
    }


    print(obj)


    try:
        print("Consultando...")
        x = requests.post(Api_URL, json = obj)
        data = json.loads(x.text)

        DATA_ALL = pd.DataFrame.from_dict(data)
        print("Realizado...")
        DATA_ALL.to_csv('result.csv')

    except:

        print("Not Found")


    DATA_ALL['Day']=DATA_ALL['Date'].apply(lambda x: ((datetime.strptime(x,'%Y-%m-%d %H:%M:%S')).strftime("%A")).upper())

    Day_names = DATA_ALL['Day']
    DATA_ALL.drop(labels=['Day'], axis=1,inplace = True)
    DATA_ALL.insert(1, 'Day', Day_names)
    DATA_ALL.insert(2, 'Dummy', "")
    DATA_ALL['Date']=DATA_ALL['Date'].apply(lambda x: x[:10])

    DATA_ALL['Week']=DATA_ALL['Date'].apply(lambda x: (datetime.strptime(x,'%Y-%m-%d')).strftime("%W"))
    Week_names = DATA_ALL['Week']
    DATA_ALL.drop(labels=['Week'], axis=1,inplace = True)
    DATA_ALL.insert(0, 'Week', Week_names)

    #print(DATA_ALL.shape)

    #Add missing columns

    for kpi in KPIS_LTE.split(","):
        if kpi not in DATA_ALL.columns:
            #print(kpi)
            DATA_ALL[kpi]=0

    #Composed KPIS

    DATA_ALL['Acc RRC All  * Acc S1  * Acc ERAB ']=DATA_ALL['LTE_5218g']*DATA_ALL['LTE_5526a']*DATA_ALL['LTE_5017a']/10000

    DATA_ALL['RAB_Traffic All RAB']=DATA_ALL[['LTE_5800e','LTE_5801e']].max(axis=1)

    column_order=['Week','Date','Day','Dummy']


    for kpi in KPI_ORDER_REPORT.split(","):
        column_order.append(kpi)

    transposed=DATA_ALL[column_order].T

    transposed.fillna(-9999, inplace=True)


    write_status=text_updater('\n**KPIS LTE:**\n'+transposed.to_string(header=False)+"\n", path_log_dir, 'a')


    transposed.to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/KPIS_LTE_"+path_log_dir[path_log_dir.rfind('/'):][7:],header=False)

    Tabla_Ref_4G[Tabla_Ref_4G['mo_distname_p'].isin(modist_sites)][['mo_distname_p','site_name']].drop_duplicates().to_csv(path_log_dir[:path_log_dir.rfind('/')]+"/Sites_Names_"+path_log_dir[path_log_dir.rfind('/'):][7:], index=False)


    return transposed





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
        print(e)
        df_ws=['ERROR']

    return df_ws


