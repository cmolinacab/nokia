
import dash
from dash.dependencies import Input, Output, State
from Nokia_Viewer_app import app

from flask import request
from sqlalchemy import create_engine

import base64
import io
import os
import os.path, time
import json
import re

import dash_core_components as dcc


from io import StringIO

from datetime import date,timedelta, datetime
import plotly.express as px



import pandas as pd
import numpy as np
import pandasql as psql


from Nokia_Viewer_functions import parse_contents, make_graph

global database_all

#Clickhouse Connection

# PM DATABASE

clickhouse_user='clickhouse_operator'
clickhouse_pass='clickhouse_operator_password'
clickhouse_url='clickhouse.claro-colombia.prod.ncs-americas.ch-dc-os-gsn-32.k8s.dyn.nesc.nokia.net'
clickhouse_port=80
clickhouse_driver='http'


connection_string = f'clickhouse://{clickhouse_user}:{clickhouse_pass}@{clickhouse_url}:{clickhouse_port}'

conn_PM = create_engine(connection_string)


path = os.getcwd()

sql_str="""

        select distinct site_name, sector_name, level_1 as region, level_2 as cluster, 'LTE' as Technology
            from default.LTE_Site_Database t
            inner join (
            select sector_name, max(export_date) as MaxDate
            from default.LTE_Site_Database
            group by sector_name
            ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate  where cluster <>'0'

        union all

        select distinct site_name, sector_name, level_1 as region, level_2 as cluster, 'LTE' as Technology
            from default.UMTS_Site_Database t
            inner join (
            select sector_name, max(export_date) as MaxDate
            from default.UMTS_Site_Database
            group by sector_name
            ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate  where cluster <>'0'

            
            """


#results = query_job.result() # Wait for the job to complete.
#database_all = results.to_dataframe(bqstorage_client=bqstorageclient)

database_all = pd.read_sql(sql_str, conn_PM)
database_all=database_all.dropna()




@app.callback(
    Output('dropdown_kpis','options'),
    Output("graph", "figure"),
    Output("error_label","hidden"),
    Output("loading-output-2", "children"),
    Output("download-dataframe-csv", "data"),
    Output('data_figure','data'),
    Output('cur_tech','data'),
    Output('dropdown_region', 'options'),
    [Input('dropdown_tech', 'value'),
    Input('button', 'n_clicks'),
    Input("graph", "figure"),
    Input('selected_kpis_global', 'data'),
    Input('start_d','data'),
    Input('end_d','data'),
    Input('df_kpis','data'),
    Input('aggregation_radio','value'),
    Input('output-data-upload', 'data'),
    Input('btn_csv', 'n_clicks'),
    Input('data_figure','data'),
    Input('aggregation_time','value'),
    Input('dropdown_cells', 'value'),
    Input('cur_cells', 'data'),], prevent_initial_call=True   
    )




def update_kpi_list(sel_tech, n_clicks, figure_map, sel_kpis, start_d, end_d, kpis_dict, aggregation, WS, n_clicks_dl, data_fig, time_agg, sel_cells, cur_cells):

    global database_all

    #Inicializar si por primera vez se subi[o un cluster
    if cur_cells==None and WS!=None:
        cur_cells=[WS[i] for i in WS]
    else:
        cur_cells=cur_cells


    show_error=True

    data_figure_all=pd.DataFrame.from_dict(data_fig)

    
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]


    
    if 'button' in changed_id:


        kpis=dash.no_update


        df=pd.DataFrame.from_dict(kpis_dict)

        df_kpis=df[df['KPI_Id'].isin([i.split(" ")[0] for i in sel_kpis])]

        #print(df_kpis)
        print(sel_cells)
        print("WS",WS)

        #figure_out, data_figure_all=make_graph(aggregation, start_d, end_d, sel_tech, [WS[i] for i in WS], df_kpis, conn_PM, bqstorageclient, time_agg)
        figure_out, data_figure_all=make_graph(aggregation, start_d, end_d, sel_tech, cur_cells, df_kpis, conn_PM,  time_agg)

        download_file=dash.no_update
        sel_tech_data=dash.no_update
        regions=dash.no_update


    elif 'btn_csv' in changed_id:


        if data_figure_all.shape[0]>0:

            df_fig_pivot=data_figure_all.pivot(index=['measTime','Element'], columns='KPI_Name', values='Value').reset_index()

            download_file=dcc.send_data_frame(df_fig_pivot.to_csv, "Data_Nokia_Viewer.csv", index=False)
        else:
            download_file=dash.no_update

        figure_out=dash.no_update
        kpis=dash.no_update
        sel_tech_data=dash.no_update
        regions=dash.no_update
 


    elif 'dropdown_tech.value' in changed_id:


        df=pd.DataFrame.from_dict(kpis_dict)


        figure_out=dash.no_update
        download_file=dash.no_update
 
        kpis=[{'label': x, 'value': x} for x in df[df['Technology']==sel_tech]['ID']]

        sel_tech_data=sel_tech

        regions=[{'label': x, 'value': x} for x in [x for x in sorted(set(database_all['region'])) if len(str(x))>2]]

        print(regions)


    else:



        figure_out=dash.no_update
        kpis=dash.no_update
        download_file=dash.no_update
        sel_tech_data=dash.no_update
        regions=dash.no_update


    #data_figure_all.to_csv('borrar.csv')

    return kpis, figure_out, show_error, "", download_file, data_figure_all.to_dict('records'), sel_tech_data, regions




@app.callback(
    Output('button', 'disabled'),
    Output('selected_kpis_global', 'data'),
    [Input('dropdown_kpis', 'value')], prevent_initial_call=True )
def display_dates(sel_kpis):


    return(False if sel_kpis is not None else True, sel_kpis)





@app.callback(

    Output('start_d','data'),
    Output('end_d','data'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')], prevent_initial_call=True )
def display_dates(start_d, end_d):

    #print(start_d, end_d)


    return start_d, end_d




@app.callback(Output('output-data-upload', 'data'),
              Output("input-box", "value"),
              Input('upload-data', 'contents'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified')
              , prevent_initial_call=True )
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:

        df_ws_pre=parse_contents(list_of_contents, list_of_names, list_of_dates)
        
        if df_ws_pre[0]=="ERROR":
            df_ws=''
            file_name='Format Error, Please upload a CSV file'
        elif len(re.findall(r"[A-Z][A-Z][A-Z].*_",str(df_ws_pre[0])))==0:
            df_ws=df_ws_pre.to_dict()
            file_name=list_of_names[0] + '  -   !!First Record does not seem to be a cell name!!'
        else:
            df_ws=df_ws_pre.to_dict()
            file_name=list_of_names[0]

    else:
        df_ws=''
        file_name=''


    return df_ws, "Current Working Set:  "+file_name + "\n\nNumber of Records:  " + str(len(df_ws))


@app.callback(
    Output('my-date-picker-range', 'max_date_allowed'),
    Output('my-date-picker-range', 'min_date_allowed'),
    Output('my-date-picker-range', 'initial_visible_month'),
    Output('my-date-picker-range', 'start_date'),
    Output('my-date-picker-range', 'end_date'),
    Output('dropdown_tech', 'disabled'),
    [Input('interval-date', 'n_intervals')] )
def update_dates(n_intervals):

    global database_all    

    #print("FECHA")

    max_date_allowed=date.today()+timedelta(days = 1)
    min_date_allowed=date(2021, 7, 4)
    initial_visible_month=date(2021, 7, 4)

    start_date=date.today() - timedelta(days=7)
    print(start_date)
    end_date=date.today()
    print(end_date)



    #Update Baseline

    sql_str="""

            select distinct site_name, sector_name, level_1 as region, level_2 as cluster, 'LTE' as Technology
                from default.LTE_Site_Database t
                inner join (
                select sector_name, max(export_date) as MaxDate
                from default.LTE_Site_Database
                group by sector_name
                ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate  where cluster <>'0'

            union all

            select distinct site_name, sector_name, level_1 as region, level_2 as cluster, 'LTE' as Technology
                from default.UMTS_Site_Database t
                inner join (
                select sector_name, max(export_date) as MaxDate
                from default.UMTS_Site_Database
                group by sector_name
                ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate  where cluster <>'0'

                
                """

    #results = query_job.result() # Wait for the job to complete.
    #database_all = results.to_dataframe(bqstorage_client=bqstorageclient)

    database_all = pd.read_sql(sql_str, conn_PM)
    database_all=database_all.dropna()


    #regions=[{'label': x, 'value': x} for x in [x for x in set(database_all['region']) if len(str(x))>2]]
                

    #print(database_all.info())


    return max_date_allowed, min_date_allowed, initial_visible_month, start_date, end_date, False




@app.callback(
    Output("download-dataframe-LTE", "data"),
    Output("download-dataframe-UMTS", "data"),
    [Input('btn_LTE', 'n_clicks'),
    Input('btn_UMTS', 'n_clicks')], 
    prevent_initial_call=True )
def display_dates(n_LTE, n_UMTS):

    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    
    if 'btn_LTE' in changed_id:

        query_job = conn_PM.query("""
           SELECT *
           FROM default.LTE_Site_Database""")


        df_bl = pd.read_sql(sql_str, conn_PM)


        ddf_LTE=dcc.send_data_frame(df_bl.to_csv, "Baseline_LTE.csv", index=False)

        ddf_UMTS=dash.no_update


    elif 'btn_UMTS' in changed_id:


        query_job = conn_PM.query("""
           SELECT *
           FROM default.UMTS_Site_Database""")


        
        df_bl = pd.read_sql(sql_str, conn_PM)


        ddf_UMTS=dcc.send_data_frame(df_bl.to_csv, "Baseline_UMTS.csv", index=False)

        ddf_LTE=dash.no_update


    else:

        ddf_LTE=dash.no_update
        ddf_UMTS=dash.no_update



    return(ddf_LTE, ddf_UMTS)



@app.callback(
    Output('dropdown_cluster', 'options'),
    Output('cur_region','data'),   
    Output('dropdown_cells', 'options'),
    Output('cur_cells','data'), 
    Output("dropdown_region", "disabled"),
    Output("dropdown_cluster", "disabled"),
    Output("dropdown_cells", "disabled"),
    Output("upload-data", "disabled"),
    Output("loading-output-3", "children"),
    Output('cur_sites','data'),
    Output("dropdown_sites", "disabled"),
    Output("dropdown_sites", "options"),
    [Input('dropdown_region', 'value'),
    Input('cur_tech','data'),
    Input('dropdown_cluster', 'value'),
    Input('cur_region','data'),
    Input('filter_radio','value'),
    Input('output-data-upload', 'data'),
    Input('dropdown_cells', 'value'),
    Input('cur_sites','data'),
    Input('dropdown_sites', 'value'),
    ], prevent_initial_call=True )
def display_dates(sel_region, sel_tech, sel_cluster, cur_region, filter_option, WS, sel_cells, cur_sites, sel_sites):

    global database_all


    sel_region='' if sel_region == None else sel_region
    sel_tech='' if sel_tech == None else sel_tech
    sel_cluster='' if sel_cluster == None else sel_cluster




    clusters=dash.no_update
    cells=dash.no_update
    sites=dash.no_update
    dis_region=dash.no_update
    dis_cluster=dash.no_update
    dis_sites=dash.no_update  
    dis_cells=dash.no_update
    dis_upload=dash.no_update
    cells_selected=dash.no_update  
    sites_selected=dash.no_update 


    print("FILTER OPTION:",filter_option)


    if filter_option=='CO':
        db_all=database_all.copy()
    elif filter_option=='BL':
        db_all=database_all.copy()
    elif filter_option=='FC':
        db_all=database_all[database_all['sector_name'].isin([WS[i] for i in WS])]




    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]

    print(changed_id)
    
    if 'dropdown_region' in changed_id:


        clusters=db_all[(db_all['region']==sel_region) & (db_all['Technology']==sel_tech) ]['cluster'].drop_duplicates()

        clusters=[{'label': x, 'value': x} for x in sorted(clusters)]

        

        cells=db_all[(db_all['region']==sel_region) & (db_all['Technology']==sel_tech) ]['sector_name'].drop_duplicates()


        cells=[{'label': x, 'value': x} for x in sorted(cells)]


        print(sel_tech)


    elif 'dropdown_cluster' in changed_id:

        cells_sql=db_all[(db_all['region']==cur_region) & (db_all['Technology']==sel_tech) & (db_all['cluster']==sel_cluster) ][['sector_name','site_name']].drop_duplicates()
        
        cells_selected=cells_sql['sector_name'].tolist()
        sites_selected=sorted(set(cells_sql['site_name']))


        cells=[{'label': x, 'value': x} for x in sorted(set(cells_sql['sector_name']))]
        sites=[{'label': x, 'value': x} for x in sorted(set(cells_sql['site_name']))]


    elif 'dropdown_sites' in changed_id:

        if sel_sites==[]:

            cells_sql=db_all[(db_all['region']==cur_region) & (db_all['Technology']==sel_tech) & (db_all['cluster']==sel_cluster) ][['sector_name','site_name']].drop_duplicates()

        else:
            cells_sql=db_all[(db_all['region']==cur_region) & (db_all['Technology']==sel_tech) & (db_all['cluster']==sel_cluster) &\
                (db_all['site_name'].isin(sel_sites))][['sector_name','site_name']].drop_duplicates()

        cells_selected=cells_sql['sector_name'].tolist()
        sites_selected=sorted(set(cells_sql['site_name']))

        cells=[{'label': x, 'value': x} for x in sorted(set(cells_sql['sector_name']))]
        #sites=[{'label': x, 'value': x} for x in sorted(set(cells_sql['site_name']))]




    elif 'dropdown_cells' in changed_id:

        print(cur_sites)


        if cur_sites==[] and sel_cells==[]:

            cells_sql=db_all[(db_all['region']==cur_region) & (db_all['Technology']==sel_tech) & (db_all['cluster']==sel_cluster) ][['sector_name','site_name']].drop_duplicates()


            cells_selected=cells_sql['sector_name'].tolist()


        elif cur_sites!=[] and sel_cells==[]:

            cells_sql=db_all[(db_all['region']==cur_region) & (db_all['Technology']==sel_tech) & (db_all['cluster']==sel_cluster) &\
                (db_all['site_name'].isin(cur_sites))][['sector_name','site_name']].drop_duplicates()

        
            cells_selected=cells_sql['sector_name'].tolist()

        else:
            cells_selected=sel_cells


    elif 'filter_radio' in changed_id:



        if filter_option =='CO':
            dis_region=True
            dis_cluster=True 
            dis_cells=True
            dis_sites=True
            dis_upload=False
            if WS is not None:
                cells_selected=[WS[i] for i in WS]
            else:
                cells_selected=sel_cells
            print(cells_selected)


        elif filter_option =='BL':

            dis_region=False
            dis_cluster=False 
            dis_cells=False
            dis_sites=False
            dis_upload=True
            cells_selected=sel_cells

        else:

            dis_region=False
            dis_cluster=False 
            dis_cells=False
            dis_sites=False
            dis_upload=False
            cells_selected=sel_cells


    elif 'output-data-upload' in changed_id:
        cells_selected=[WS[i] for i in WS]


   
        print("WS SUBIDO")



    return clusters, sel_region, cells, cells_selected, dis_region, dis_cluster, dis_cells, dis_upload, "", sites_selected, dis_sites, sites






