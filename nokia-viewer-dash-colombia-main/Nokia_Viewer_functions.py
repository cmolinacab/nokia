import base64
import io
import os
import os.path, time
import json

from io import StringIO


import plotly.graph_objs as go


from datetime import date,timedelta, datetime
import plotly.express as px

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table





import pandas as pd
import numpy as np
import requests

import math
import itertools
from shapely.geometry import Polygon




from functools import reduce
import operator




def parse_contents(contents, filename, date):

    #print(contents,filename, date)

    content_type, content_string = contents[0].split(',')

    decoded = base64.b64decode(content_string)
    #print(filename[0])
    try:
        if 'csv' in filename[0]:
            # Assume that the user uploaded a CSV file
            df_ws = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')), header=None)[0]



            
            

            #df_site=df_ws.merge(pd.DataFrame.from_dict(site_db), left_on=0, right_on='site_name')[['sector_name']]
            #df_site.columns=[0]

            

            #df_ws=pd.concat([df_site,df_ws]).drop_duplicates().reset_index(drop=True)[0]






        else:

            df_ws=['ERROR']



    except Exception as e:
        print(e)
        df_ws=['ERROR']

    return df_ws





def create_graph(df_all):

    from sklearn.cluster import KMeans

    
    if df_all.shape[0]>0:

        df_all['Value']=df_all['Value'].astype(float)
 
        max_results=df_all.groupby('KPI').agg({'Value':'max'}).reset_index().dropna()

        max_results.replace(to_replace=0, value=0.1, inplace=True)

        if max_results.shape[0]>1:

            kmeans = KMeans(n_clusters=2)

            y = kmeans.fit_predict(np.log10(np.abs(max_results[['Value']])))



            max_results['Cluster'] = y
        else:
            max_results['Cluster'] = 0

        df_all=df_all.merge(max_results[['KPI','Cluster']],left_on='KPI',right_on='KPI')

        result_y1=df_all[df_all['Cluster']==0]
        result_y2=df_all[df_all['Cluster']==1]




        subfig = make_subplots(specs=[[{"secondary_y": True}]])

        figure=px.line(result_y1,
                                 y='Value',
                                 x='measTime',
                                 color='Name_Legend')




        if result_y2.shape[0]>0:

            figure2=px.line(result_y2,
                                     y='Value',
                                     x='measTime',
                                     color='Name_Legend')


            figure2.update_traces(yaxis="y2")

            subfig.add_traces(figure.data + figure2.data)
        else:
            
            subfig.add_traces(figure.data)



        subfig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color, width=4)))

        subfig.update_layout(legend=dict(
            orientation="h",
            #yanchor="bottom",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0

        ), paper_bgcolor="white", plot_bgcolor='white'
        #hovermode='x'
        )

        subfig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='grey')
        #subfig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='grey')

        #print(df_all)

        return(subfig)





def make_graph(aggregation, date_ini, date_end, tech, WS, df_kpis, conn_PM,  time_agg):

    #print(df_kpis)

    if len(WS)>0:

        date_ini = datetime.strptime(date_ini, '%Y-%m-%d')
        date_ini=(date_ini + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S")

        date_end = datetime.strptime(date_end, '%Y-%m-%d')
        date_end=(date_end + timedelta(hours=24+5)).strftime("%Y-%m-%d %H:%M:%S")    



        start_time = time.time()

        df_bq_all=pd.DataFrame()


        for index, row in df_kpis.iterrows():

            if row['Level']=='sector_name':

                
                if row['Formula'][:4]=="100-":
                    prefix=row['Formula'][:4]
                    formula=row['Formula'][4:]
                else:
                    prefix=""
                    formula=row['Formula']     
                
                if "/" in formula and "LOG" not in formula:
                    formula=prefix+formula#prefix+"SAFE_DIVIDE("+formula.replace("/",",")+")"
                else:
                    formula=prefix+formula
                    
                
                cur_kpi=row['KPI_Id']


                sql_str=f"""SELECT  DATE_TRUNC ('{time_agg}',a.measTime, 'America/Bogota') as measTime


                {aggregation} as Element, {formula} as Value
                FROM {row['Tables']}, 
                
                
                (select t.export_date, t.moDistName, t.sector_name, t.site_name, t.level_2 as cluster, t.band
                from default.{row['Technology']}_Site_Database t
                inner join (
                select sector_name, max(export_date) as MaxDate
                from default.{row['Technology']}_Site_Database
                group by sector_name
                ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate) x

                
                WHERE {row['Join']} a.measTime>='{date_ini}' AND a.measTime<='{date_end}'
                AND a.distName=x.moDistName
                AND x.sector_name IN 
                (
                {str(WS)[1:-1]}
                )
                GROUP BY measTime, Element order by measTime"""

                #print(sql_str)
                
                #print("\n",row['Name'])


            elif row['Level']=='site_name':

                WS_Site = sorted(set([i[:i.find('_')] for i in WS]))


                if row['Formula'][:4]=="100-":
                    prefix=row['Formula'][:4]
                    formula=row['Formula'][4:]
                else:
                    prefix=""
                    formula=row['Formula']     
                
                if "/" in formula and "LOG" not in formula:
                    formula=prefix+formula#prefix+"SAFE_DIVIDE("+formula.replace("/",",")+")"
                else:
                    formula=prefix+formula
                    
                
                cur_kpi=row['KPI_Id']


                sql_str=f"""SELECT 
                DATE_TRUNC ('{time_agg}',a.measTime, 'America/Bogota') as measTime


                {aggregation} as Element, {formula} as Value
                FROM {row['Tables']}, 
                
                
                (select distinct 
                substring(t.moDistName, 1, position(t.moDistName, '/LNCEL')-1) as moDistName,
                t.site_name as sector_name, t.site_name, t.level_2 as cluster, t.band
                from default.{row['Technology']}_Site_Database t
                inner join (
                select site_name, max(export_date) as MaxDate
                from default.{row['Technology']}_Site_Database
                group by site_name
                ) tm on t.site_name = tm.site_name and t.export_date = tm.MaxDate) x

                
                WHERE {row['Join']} a.measTime>='{date_ini}' AND a.measTime<='{date_end}'
                AND a.distName=x.moDistName
                AND x.sector_name IN 
                (
                {str(WS_Site)[1:-1]}
                )
                GROUP BY measTime, Element order by measTime"""

                print(sql_str)
                
                #print("\n",row['Name'])













                
            #query_job = client.query(sql_str)

            #results = query_job.result()
            print(sql_str)
            
            df_bigquery= pd.read_sql(sql_str, conn_PM)#results.to_dataframe(bqstorage_client=bqstorageclient)

            df_bigquery['KPI']=cur_kpi
            df_bigquery['Name']=row['Name']

            if df_bigquery.shape[0]>0:

                df_bigquery['Element_KPI']=df_bigquery.apply(lambda row: str(row['KPI'])+"_"+str(row['Element']), axis=1)
                df_bigquery['KPI_Name']=df_bigquery.apply(lambda row: str(row['KPI'])+" - "+str(row['Name']), axis=1)
                df_bigquery['Name_Legend']=df_bigquery.apply(lambda row: str(row['KPI'])+" - "+str(row['Name']) +"_"+str(row['Element']), axis=1)



            #COncatenar resultados

                df_bq_all=pd.concat([df_bq_all,df_bigquery])

        #Create Graph

        

        if df_bq_all.shape[0]>0:

            figure_kpi=create_graph(df_bq_all)


        else:
            figure_kpi=dash.no_update

    else:
        figure_kpi=dash.no_update        

        



    return(figure_kpi, df_bq_all[['measTime','Element', 'Value', 'KPI', 'KPI_Name']])
