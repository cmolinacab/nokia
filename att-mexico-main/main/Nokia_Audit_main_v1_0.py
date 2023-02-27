import base64
import io
import os
import os.path, time
import json
import random
from pathlib import Path
import prestodb
from sqlalchemy import create_engine
import re

from io import StringIO

from datetime import date,timedelta, datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


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


from Nokia_Audit_functions import Param_Template_Audit, get_site_list, text_updater, \
Audit_SW_Load_Version, PCI_RSI_Audit, RTWP_Audit, TAC_LAC_Audit, LNRELW_Audit, RSSI_Audit, RET_Audit,\
LTE_KPIS_Hist, parse_contents, send_mail, update_template_vars

from Nokia_Audit_functions_Final_Report import final_report, final_report_kpi



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
    port=8080,
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
clickhouse_port='8123'
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



cur_version=1.3

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



    html.Div([


        html.P("SEND MAIL TO:"),
        dcc.Dropdown(
            id='dropdown_mail',
            options=[
                {'label': x, 'value': x}
                for x in []#receivers_email.split(",")
            ],
            multi=True,
        ),

        html.P("  "),
        html.P("\nSITES:"),
        dcc.Dropdown(
            id='dropdown_sites',
            options=[
                {'label': x+"  -  "+y, 'value': y}
                for x,y in []
            ],
            multi=True,
        ),
        html.Div(id='dd-output-container')
        

        ],
        style={'width': '90%', 'display': 'inline-block'}),



    html.P("  "),
    html.P("  "),





    html.Br(),
    html.P("OR .... Upload your Cluster:"),
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

    
    dcc.RadioItems(
            id="filter_radio",
            options=[
            {'label': 'Auditoría Completa  ', 'value': 'AC'},
            {'label': '  Solo KPIs  ', 'value': 'SK'}
                ],
        value='AC',
        labelStyle={'display': 'inline-block'}
    ),

    html.P("  "),
    html.P("  "),
    html.Button('Generate Audit', id='button', disabled=False),
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

    dcc.Interval(id='interval-update-template', interval=900000, n_intervals=0)




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
    Output("dropdown_mail","options"),
    Output("dropdown_sites","options"),
    Input('dropdown_sites', 'value'),
    Input('button', 'n_clicks'),
    Input('cur_sites', 'data'),
    Input('initial_file_name', 'data'),
    Input('permanent_file_name', 'data'),
    Input('interval-update', 'n_intervals'),
    Input('output-data-upload', 'data'),
    Input("initial_path_log", "data"),
    Input('filter_radio','value'),
    Input('dropdown_mail', 'value')
    )
def execute_functions(sel_sites, n_clicks, cur_sites,  path_text_update, file, n, sel_sites_ws, log_timestamp_dir, filter_option, sel_mails):


    global LNBTS_version
#    global LTE_Template_Path
    global Logs_Path



    clear_data=False

    if sel_sites_ws and not sel_sites:

        sel_sites=[sel_sites_ws[i] for i in sel_sites_ws]



    text_update=dash.no_update
    bar_status=dash.no_update
    receivers_email_list=dash.no_update
    site_list=dash.no_update


    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]

    if path_text_update=="":

        #Se actualizan las variables desde el s3
        LNBTS_version,RTWP_Port_Dif,RTWP_Threshold,RSSI_Th_Outdoor,RSSI_Th_Indoor,Audit_Template,Audit_Template_KPI,\
        Logs_Path,Share_HO_PCS_RSI,LTE_KPIS,KPI_ORDER_REPORT,FM_Exceptions,FM_VSWR,FM_PIM,receivers_email=update_template_vars(s3_bucket,s3)



        #Se actualizan los sitios desde clickhouse

        site_list=[
                {'label': x+"  -  "+y, 'value': y}
                for x,y in get_site_list(conn)
            ]
        

        #print(site_list)


    
        receivers_email_list=receivers_email.split(",")

        path_text_update=Logs_Path

        log_timestamp_dir=datetime.strftime(datetime.today() , "%HH-%MM-%SS_")+str(random.randint(1000, 99999))

        path_log_dir=path_text_update + log_timestamp_dir

        Path(path_log_dir).mkdir(parents=True, exist_ok=True)

        path_text_update=path_log_dir+"/Session_Control_"+log_timestamp_dir+".csv"

        write_status=text_updater("", path_text_update, 'w')

        print(log_timestamp_dir)


    if 'button' in changed_id:

        LNBTS_version,RTWP_Port_Dif,RTWP_Threshold,RSSI_Th_Outdoor,RSSI_Th_Indoor,Audit_Template,Audit_Template_KPI,\
        Logs_Path,Share_HO_PCS_RSI,LTE_KPIS,KPI_ORDER_REPORT,FM_Exceptions,FM_VSWR,FM_PIM,receivers_email=update_template_vars(s3_bucket,s3)


        bar_status={'display': 'block'}

        
        print('path_text_update:',path_text_update)

        log_timestamp=datetime.strftime(datetime.today() , "%HH-%MM-%SS_")+str(random.randint(1000, 99999))

        audit_log_file=path_text_update[:path_text_update.rfind("/")]+"/Audit_Log_"+log_timestamp+".csv"

        print('Audit lo file:', audit_log_file)

        write_status=text_updater("", audit_log_file, 'w')

        write_status=text_updater("\n"+audit_log_file, path_text_update, 'a')


        update_output=''

        version=LNBTS_version

        print("Filter Option=",filter_option)

        if filter_option =='AC':

            

            #TAC LAC
            try:
                write_status=text_updater("**Ejecutando Auditoria TAC LAC...**\n", audit_log_file, 'w')
                Audit_TAC_LAC=TAC_LAC_Audit(sel_sites, audit_log_file, conn, conn_PM)

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')
            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')





            #KPIS
            try:
                write_status=text_updater("\n\n**Obteniendo KPIS 10 dias...Este proceso suele demorar un tiempo!**\n", audit_log_file, 'a')

                LTE_KPIS_Audit=LTE_KPIS_Hist(sel_sites, audit_log_file, conn_PM, LTE_KPIS, api_url, KPI_ORDER_REPORT)

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            #RET
            try:

                write_status=text_updater("\n\n**Ejecutando Auditoria RET...**\n", audit_log_file, 'a')

                Audit_RET=RET_Audit(sel_sites, audit_log_file, conn, conn_PM)

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')
            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            #RSSI



            write_status=text_updater("**Ejecutando Auditoria RSSI...**\n", audit_log_file, 'a')

            try:

                Audit_RSSI=RSSI_Audit(sel_sites, audit_log_file, conn_PM, RSSI_Th_Outdoor, RSSI_Th_Indoor, api_url)

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')
            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')



            #LNRELW
            try:

                write_status=text_updater("**Ejecutando Auditoria LNRELW...**\n", audit_log_file, 'a')

                Audit_LNRELW=LNRELW_Audit(sel_sites, audit_log_file, conn, conn_PM)

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')




            #RTWP Audit

            write_status=text_updater("**Ejecutando Auditoria RTWP...**\n", audit_log_file, 'a')

            try:
                Audit_RTWP_Diff, Audit_RTWP_Th=RTWP_Audit(sel_sites, audit_log_file, conn_PM, RTWP_Port_Dif, RTWP_Threshold)

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')




            #PCI RSI Audit

            write_status=text_updater("\n**Ejecutando Auditoria PCI RSI...**\n", audit_log_file, 'a')

            try:
                PCI_Conflicts, RSI_Conflicts=PCI_RSI_Audit(sel_sites, audit_log_file, conn, conn_PM, Share_HO_PCS_RSI)

            
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')




            #SW Version Audit

            write_status=text_updater("\n**Ejecutando Auditoria SW Load Version...**\n", audit_log_file, 'a')

            try:
                result=Audit_SW_Load_Version(version, sel_sites, audit_log_file, conn)
                update_output=update_output+result.to_string(index=False)
                write_status=text_updater(update_output, audit_log_file, 'a')

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')
            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')




            #Parameter Template Audit

            write_status=text_updater("\n**Ejecutando Auditoria de Parametros segun Template...**\n", audit_log_file, 'a')

            try:
                app_path = os.getcwd()
                LTE_Template_Path = f"{app_path}/Param_Template_Simple_LTE.csv"
                LTE_Template=pd.read_csv(LTE_Template_Path, sep=";")
                Template_ALL=LTE_Template
                Template_ALL['Value'] = Template_ALL['Value'].astype(str)

                summary, param_dev_ALL=Param_Template_Audit(Template_ALL,sel_sites, audit_log_file, conn)
                update_output=summary.to_string(index=False)

                write_status=text_updater('\n', audit_log_file, 'a')

                write_status=text_updater(update_output, audit_log_file, 'a')

                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            except:
                write_status=text_updater('\nError al ejecutar auditoría\n', audit_log_file, 'a')
                write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            
            #Reporte Final 

            write_status=text_updater("\n**Generando REPORTE FINAL...**\n", audit_log_file, 'a')

            print(log_timestamp_dir, log_timestamp)

            status_FR=final_report(log_timestamp_dir, log_timestamp, Audit_Template, Logs_Path)

            write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')


            write_status=text_updater("\n**FIN DE LA AUDITORIA!**\n", audit_log_file, 'a')

        else: #Solo KPIs


            #KPIS

            write_status=text_updater("\n\n**Obteniendo KPIS 10 dias...Este proceso suele demorar un tiempo!**\n", audit_log_file, 'w')

            LTE_KPIS_Audit=LTE_KPIS_Hist(sel_sites, audit_log_file, conn_PM, LTE_KPIS, api_url, KPI_ORDER_REPORT)

            write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')

            #Reporte FInal KPI

            write_status=text_updater("\n**Generando REPORTE FINAL...**\n", audit_log_file, 'a')

            print('Dir de rep final',log_timestamp_dir, log_timestamp)

            status_FR=final_report_kpi(log_timestamp_dir, log_timestamp, Audit_Template_KPI, Logs_Path)

            write_status=text_updater('\n'+'_'*120+'\n', audit_log_file, 'a')


            write_status=text_updater("\n**FIN DE LA AUDITORIA!**\n", audit_log_file, 'a')


        #Send Mail to receivers

        status_mail=send_mail(log_timestamp_dir, log_timestamp, sel_mails, 'tools.ncs_lat@nokia.com', Logs_Path, filter_option)

        text_update=dash.no_update
        clear_data=True
        path_text_update=""
        log_timestamp_dir=""
        bar_status=dash.no_update





    elif 'interval-update' in changed_id:

        #print(file)
        if file!=None and file!='':
            bar_status={'display': 'block'}

            last_line=''
            line=''

            f= open(file, 'r')

            for line in f:
                pass
            last_line = line

            if last_line!="":

                #print("Lleno", last_line)
                reader= open(last_line, 'r')
                text_update=reader.read()
                #print("ESTO VA",text_update)
                reader.close()
        else:
            text_update=dash.no_update



    else:


        
        cur_sites=dash.no_update
        text_update=dash.no_update
        #path_text_update=dash.no_update

  
    #print("Salida: ", site_list)
    return  sel_sites, path_text_update, clear_data, path_text_update, clear_data, bar_status, text_update, log_timestamp_dir, receivers_email_list, site_list


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
        #elif len(re.findall(r"[A-Z][A-Z][A-Z].*_",str(df_ws_pre[0])))==0:
        elif not re.match('^PLMN-PLMN\/MRBTS-[\w]*\/LNBTS-[\w]*$',df_ws_pre[0]):
            df_ws=df_ws_pre.to_dict()
            file_name=list_of_names + '  -   !!First Record does not seem to be a LTE Site mo_distName!!'
        else:
            df_ws=df_ws_pre.to_dict()
            file_name=list_of_names

    else:
        df_ws=''
        file_name=''


    return df_ws, "Current Working Set:  "+file_name + "\n\nNumber of Records:  " + str(len(df_ws))



if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')

