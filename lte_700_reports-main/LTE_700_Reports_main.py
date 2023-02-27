import base64
import io
import os
import os.path, time
import json
import random
from pathlib import Path
import prestodb
import re

from io import StringIO
from time import sleep
from datetime import date,timedelta, datetime

from sqlalchemy import create_engine

import flask

from flask import request

import dash
from dash import no_update
from dash.dependencies import Input, Output, State
from dash import dash_table
import dash_auth
import dash_html_components as html
import dash_core_components as dcc


import pandas as pd
import numpy as np
import pandasql as psql


from LTE_700_Reports_functions import text_updater, parse_contents, LTE_LNCEL_Report, LTE_LNBTS_Report, UMTS_WCEL_Report, UMTS_WBTS_Report, GSM_BTS_Report

import time
import json

import boto3


#from Nokia_Audit_app import app
#import Nokia_Audit_callbacks

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, update_title=None, title='NOKIA Audit')






global LNBTS_version
#global LTE_Template_Path
global Logs_Path


#************************************************************************************************************

#API Performance Monitor

api_url=f"http://{os.getenv('pm_api')}/api/calculate_kpi"


#Presto Connection

conn = prestodb.dbapi.connect(
    host=os.getenv('presto_host'),
    port=os.getenv('presto_port'),
    user='admin',
    catalog='hive',
    schema='network_data'
)

#Clickhouse Connection

# PM DATABASE

time_zone=os.getenv('time_zone', 'UTC')

clickhouse_user=os.getenv('clickhouse_user')
clickhouse_pass=os.getenv('clickhouse_pass')
clickhouse_url=os.getenv('clickhouse_url')
clickhouse_port=os.getenv('clickhouse_port')
clickhouse_driver='http'


connection_string = f'clickhouse://{clickhouse_user}:{clickhouse_pass}@{clickhouse_url}:{clickhouse_port}'

conn_PM = create_engine(connection_string)




# S3 Connection

s3_credentials = json.loads(str(os.environ['s3_credentials']))

s3_bucket = s3_credentials["bucket"]

s3 = boto3.session.Session().client(
    service_name='s3',
    aws_access_key_id=s3_credentials["access_key"],
    aws_secret_access_key=s3_credentials["secret_key"],
    endpoint_url=s3_credentials["endpoint"])


#************************************************************************************************************








#print(site_list)

# Create instances of a flask web framework 



cur_version=1.0

VALID_USERNAME_PASSWORD_PAIRS = {
    'nokia': 'nokia'
}

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


path = os.getcwd()






#START DESIGNING WEB PAGE

tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}







app.layout = html.Div([

    html.Div([

    html.Img(src=app.get_asset_url('Nokia-Logo.png'))

    ],
    style={'width': '68%', 'display': 'inline-block'}),



    html.Div([

        dcc.Loading(
            id="loading-1",
            children=[html.Div([html.Div(id="loading-output-1")])],
            type="default",
        )

    ],
    style={'width': '15%', 'display': 'none'}), 
    #style={'width': '15%', 'display': 'inline-block'}), 

    html.Div([
    html.H6("v"+str(cur_version), 
        style={
            'textAlign': 'right',
            'color': 'red'
        }, hidden=False)

    ],
    style={'width': '15%', 'display': 'inline-block'}),


    html.Div([

    
    html.H6("NO RESULTS FOR THE TIME PERIOD SELECTED", id="error_label", 
        style={
            'textAlign': 'left',
            'color': 'red'
        }, hidden=True)

    ],
    style={'width': '100%', 'display': 'inline-block'}),









    html.P("  "),
    html.P("  "),





    html.Br(),
    #html.P("OR .... Upload your Cluster:"),
    html.P("  "),

    html.Div([
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ,        

        ]),
        style={'display': 'inline-block',
            'width': '80%',
            'height': '65px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': 'auto'
        },
        # Allow multiple files to be uploaded
        multiple=False
    ),
    #html.P("  "),
    
    #dcc.Input(id='input-box', type='text'),
    

    ],
    style={'width': '40%',  'display': 'inline-block'}),

    html.Div([

        dcc.Textarea(
        id='input-box',
        placeholder='Enter a value...',
        value='Current Working Set:  ',
        disabled=True,
        style={'width': '80%','height': 20, 'color': 'red'}
        ),

    ],
    style={'width': '40%', 'display': 'inline-block'}),





#Grafico    
    html.P("  "),

    html.Div([
    html.Div(children="History Days:", id="hist_label", style={'color': 'black', 'fontSize': 18}),
    ],  
    style={'width': '50%',  'display': 'inline-block', 'textAlign': 'right'}),

    html.Div([
    dcc.Slider(min=1, max=180, step=1, value=10, id='hist_slider', marks=None, tooltip={"placement": "bottom", "always_visible": True}),
    ],
    style={'width': '38%',  'display': 'inline-block'}),



    html.P("  "),
    html.Button('Run Reports', id='button', disabled=False),
    html.P("  "),
    html.P("  "),




    dcc.Markdown(id="text_dummy", style={"white-space": "pre"}, children='''  \n'''),
    dcc.Markdown(id="text_md", style={"white-space": "pre"}, children='''...'''),



    dcc.Store(id='cur_sites', data=''),
    dcc.Store(id='initial_file_name', data=''),
    dcc.Store(id='initial_path_log', data=''),
    dcc.Store(id='permanent_file_name', data=''),
    dcc.Store(id='output-data-upload'),

    dcc.Interval(id='interval-update', interval=5000, n_intervals=0),


    #Aqui se guarda el valor del slider de historia
    dcc.Store(id='hist_slider_dcc'),

    ])



@app.callback(
    Output('cur_sites','data'),
    Output('initial_file_name', 'data'),
    Output('initial_file_name', 'clear_data'),
    Output('permanent_file_name', 'data'),
    Output('permanent_file_name', 'clear_data'),
    Output("loading-output-1", component_property='style'),
    Output("text_md", "children"),
    Output("initial_path_log", "data"),
    Input('button', 'n_clicks'),
    Input('cur_sites', 'data'),
    Input('initial_file_name', 'data'),
    Input('permanent_file_name', 'data'),
    Input('interval-update', 'n_intervals'),
    Input('output-data-upload', 'data'),
    Input("initial_path_log", "data"),
    Input('hist_slider_dcc', 'data')
    )
def execute_functions(n_clicks, cur_sites,  path_text_update, file, n, sel_sites_ws, log_timestamp_dir, hist_days):


    global LNBTS_version
    global Logs_Path


    sel_sites=False
    clear_data=False

    if sel_sites_ws and not sel_sites:

        sel_sites=[sel_sites_ws[i] for i in sel_sites_ws]


    text_update=dash.no_update
    bar_status=dash.no_update
    site_list=dash.no_update


    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]


    if path_text_update=="":


        Logs_Path=os.getcwd()+'/Logs/'

        print('Logs_Path', Logs_Path)

        path_text_update=Logs_Path

        log_timestamp_dir=datetime.strftime(datetime.today() , "%HH-%MM-%SS_")+str(random.randint(1000, 99999))

        print('log_timestamp_dir', log_timestamp_dir)

        path_log_dir=path_text_update + log_timestamp_dir

        print("Generar directorio en :", path_log_dir)

        Path(path_log_dir).mkdir(parents=True, exist_ok=True)

        path_text_update=path_log_dir+"/Session_Control_"+log_timestamp_dir+".csv"

        write_status=text_updater("", path_text_update, 'w')

        print(log_timestamp_dir)



    if 'button' in changed_id:


        bar_status={'display': 'block'}

        start_time = time.time()

        
        print('path_text_update:',path_text_update)

        log_timestamp=datetime.strftime(datetime.today() , "%HH-%MM-%SS_")+str(random.randint(1000, 99999))

        audit_log_file=path_text_update[:path_text_update.rfind("/")]+"/Audit_Log_"+log_timestamp+".csv"

        print('Audit lo file:', audit_log_file)

        write_status=text_updater("", audit_log_file, 'w')

        write_status=text_updater("\n"+audit_log_file, path_text_update, 'a')


        update_output=''

        write_status=text_updater(f"**Sus reportes estarán ubicados en el S3: /claro-colombia-production/Reports/LTE_700/{log_timestamp}**", audit_log_file, 'w')

            #MULTIPROCESO


        import multiprocessing as mp
        processes=[]



    ##################



        #GSM BTS

        #elapsed_time=GSM_BTS_Report(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)
 
        processes.append(mp.Process(target=GSM_BTS_Report, args=(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)))  
        write_status=text_updater("\n\n**Ejecutando Reporte GSM BTS...**\n", audit_log_file, 'a')

        #UMTS WBTS

        #elapsed_time=UMTS_WBTS_Report(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)

        processes.append(mp.Process(target=UMTS_WBTS_Report, args=(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)))  
        write_status=text_updater("\n\n**Ejecutando Reporte UMTS WBTS...**\n", audit_log_file, 'a')

        #UMTS WCEL

        #elapsed_time=UMTS_WCEL_Report(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)

        processes.append(mp.Process(target=UMTS_WCEL_Report, args=(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)))  
        write_status=text_updater("\n\n**Ejecutando Reporte UMTS WCEL...**\n", audit_log_file, 'a')

        #LTE LNCEL
        
        #elapsed_time=LTE_LNCEL_Report(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)

        processes.append(mp.Process(target=LTE_LNCEL_Report, args=(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)))  
        write_status=text_updater("\n\n**Ejecutando Reporte LTE LNCEL...**\n", audit_log_file, 'a')

        #LTE LNBTS

        #elapsed_time=LTE_LNBTS_Report(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)

        processes.append(mp.Process(target=LTE_LNBTS_Report, args=(api_url, conn, conn_PM, s3, sel_sites_ws, int(hist_days), log_timestamp)))  
        write_status=text_updater("\n\n**Ejecutando Reporte LTE LNBTS...**\n", audit_log_file, 'a')


        write_status=text_updater(f"\n\n**'Procesos en Paralelo:\n' {processes}", audit_log_file, 'a')

        sleep(6)

        # Run processes
        for p in processes:
            p.start()

        # Exit the completed processes
        for p in processes:
            p.join()

        write_status=text_updater(f"\n\nTiempo Total: {(time.time()-start_time)/60} minutes", audit_log_file, 'a')

        

 
        text_update=dash.no_update
        clear_data=True
        path_text_update=""
        log_timestamp_dir=""
        bar_status=dash.no_update





    elif 'interval-update' in changed_id:


        if file!=None and file!='':
            bar_status={'display': 'block'}

            last_line=''
            line=''

            f= open(file, 'r')

            for line in f:
                pass
            last_line = line

            if last_line!="":

                reader= open(last_line, 'r')
                text_update=reader.read()

                reader.close()
        else:
            text_update=dash.no_update



    else:


        
        cur_sites=dash.no_update

    return  sel_sites, path_text_update, clear_data, path_text_update, clear_data, bar_status, text_update, log_timestamp_dir




@app.callback(Output('output-data-upload', 'data'),
              Output("input-box", "value"),
              Input('upload-data', 'contents'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified')
              , prevent_initial_call=True )
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:

        print("\n",list_of_contents,"\n")

        df_ws_pre=parse_contents(list_of_contents, list_of_names, list_of_dates)

        print(df_ws_pre[0])
        
        if df_ws_pre[0]=="ERROR":
            df_ws=''
            file_name='Format Error, Please upload a CSV file'

        else:
            df_ws=df_ws_pre.to_dict()
            file_name=list_of_names
            print(df_ws)

    else:
        df_ws=''
        file_name=''


    return df_ws, "Current Working Set:  "+file_name + "\n\nNumber of Records:  " + str(len(df_ws))

## Slider

@app.callback(
    Output('hist_slider_dcc', 'data'),
    Input('hist_slider', 'value'))

def update_histiry(value):
    print("Historia:",value)
    return value

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')

