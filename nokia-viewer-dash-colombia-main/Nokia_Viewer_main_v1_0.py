import base64
import io
import os
import os.path, time
import json

from sqlalchemy import create_engine

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
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import dash_auth


import pandas as pd
import numpy as np
import pandasql as psql


from Nokia_Viewer_functions import parse_contents, make_graph



import time
import json

from Nokia_Viewer_app import app
import Nokia_Viewer_callbacks


# Create instances of a flask web framework 



cur_version=1.0



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']




path = os.getcwd()




#Clickhouse Connection

# PM DATABASE

clickhouse_user='clickhouse_operator'
clickhouse_pass='clickhouse_operator_password'
clickhouse_url='clickhouse.claro-colombia.prod.ncs-americas.ch-dc-os-gsn-32.k8s.dyn.nesc.nokia.net'
clickhouse_port=80
clickhouse_driver='http'


connection_string = f'clickhouse://{clickhouse_user}:{clickhouse_pass}@{clickhouse_url}:{clickhouse_port}'

conn_PM = create_engine(connection_string)






# Site Database all Technologies


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

print(database_all.head(20))


site_db_dict=database_all.to_dict('records')



# KPIS actuales en BigQuery


sql_str="""
   SELECT *
   FROM Visualization.NOKIA_KPIS"""

kpis_all = pd.read_sql(sql_str, conn_PM)

print(kpis_all.head(20))


#results = query_job.result() # Wait for the job to complete.
#kpis_all = results.to_dataframe(bqstorage_client=bqstorageclient)

kpis_all=kpis_all.fillna('')

kpis_all['ID']=kpis_all['KPI_Id']+" - "+kpis_all['Name']

df_dict=kpis_all.to_dict('records')





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
    style={'width': '15%', 'display': 'inline-block'}),

    html.Div([
    html.H6("v"+str(cur_version), 
        style={
            'textAlign': 'right',
            'color': 'red'
        }, hidden=False)

    ],
    style={'width': '15%', 'display': 'inline-block'}),

#TABS
    dcc.Tabs([

    #LTE TAB

        dcc.Tab(label='NOKIA VIEWER', style=tab_style, selected_style=tab_selected_style, children=[

    html.Div([

    
    html.H6("NO RESULTS FOR THE TIME PERIOD SELECTED", id="error_label", 
        style={
            'textAlign': 'left',
            'color': 'red'
        }, hidden=True)

    ],
    style={'width': '100%', 'display': 'inline-block'}),

    html.Div([
    

        html.P("Technology:"),
        dcc.Dropdown(
            id="dropdown_tech", 
            options=[
                {'label': x, 'value': x}
                for x in set(kpis_all['Technology'])
            ],
            multi=False,
            clearable=False,
            disabled=True
        ),
        
        html.Div([
        html.P("  "),
        html.P("  "),
        html.P("Select Period:"),   
        html.P("  "),
        html.P("  "), 
        dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=date(2021, 7, 4),
        max_date_allowed=date.today()+timedelta(days = 1),
        initial_visible_month=date(2021, 7, 4),
        start_date=date.today() - timedelta(days=7),
        end_date=date.today()
        ),
 ])

    ],
    style={'width': '20%', 'display': 'inline-block'}), 

    html.Div([
    

        html.P("KPIs:"),
        dcc.Dropdown(
            id="dropdown_kpis",
            options=[
                {'label': x, 'value': x}
                for x in []
            ],
            multi=True,
            clearable=False,
        ),
        html.P("  "),
        html.P("Select Object Aggregation:"),   
        html.P("  "),


        dcc.RadioItems(
                id="aggregation_radio",
                options=[
                {'label': 'Whole Cluster', 'value': ", 'WS'"},
                {'label': 'Cluster CRC  ', 'value': ', x.cluster'},
                {'label': 'Sector Name  ', 'value': ', x.sector_name'},
                {'label': 'Site Name  ', 'value': ', x.site_name'},
                {'label': 'Band  ', 'value': ', x.band'}
                    ],
            value=", 'WS'",
            labelStyle={'display': 'inline-block'}
        ),
        html.P("  "),
        html.P("Select Time Aggregation:"),   
        html.P("  "),


        dcc.RadioItems(
                id="aggregation_time",
                options=[
                {'label': 'Hour', 'value': 'hour'},
                {'label': 'Day  ', 'value': 'day'},
                {'label': 'Month  ', 'value': 'month'},
                {'label': 'Week  ', 'value': 'week'},
                {'label': 'Quarter  ', 'value': 'quarter'},
                {'label': 'Year  ', 'value': 'year'}
                    ],
            value='hour',
            labelStyle={'display': 'inline-block'}
        ),


        html.P("  "),
        html.P("Select Cluster Filter Option:"),   
        html.P("  "),
        
        dcc.RadioItems(
                id="filter_radio",
                options=[
                {'label': 'Cluster Only', 'value': 'CO'},
                {'label': 'Baseline  ', 'value': 'BL'},
                {'label': 'Filter BL with Cluster  ', 'value': 'FC'}
                    ],
            value='CO',
            labelStyle={'display': 'inline-block'}
        ),
        html.Div([
        html.P("  "),
        html.P("  ")

        

        ],
        style={'width': '20%', 'display': 'inline-block'}),


        html.Div([

            dcc.Loading(
                id="loading-2",
                children=[html.Div([html.Div(id="loading-output-2")])],
                type="default",
            ),
            dcc.Loading(
                id="loading-dropdown",
                children=[html.Div([html.Div(id="loading-output-3")])],
                type="default",
            )

        ],
        style={'width': '50%', 'display': 'inline-block'})

    ],
    style={'width': '79%', 'float': 'right', 'display': 'inline-block'}), 




    #Grafico
    html.P("  "),
    html.P("  "),
 
    
    html.P("  "),
    html.P("  "),
    html.Br(),
    html.Br(),
    html.Br(),
    html.P("Upload your Cluster:"),
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
        multiple=True
    ),
    #html.P("  "),
    
    #dcc.Input(id='input-box', type='text'),
    

    ],
    style={'width': '40%',  'display': 'inline-block'}),






#Dropdown de region, cluster y celda

    html.Div([
    


        html.P("Region:"),
        dcc.Dropdown(
            id="dropdown_region",
            options=[
                {'label': x, 'value': x}
                for x in []#[x for x in set(database_all['region']) if len(str(x))>2]
            ],
            placeholder="Select a Region",
            multi=False,
            clearable=False,
            disabled=True
        ),

    ],
    style={'width': '10%', 'display': 'inline-block'}), 


    html.Div([
    

        html.P("Cluster:"),
        dcc.Dropdown(
            id="dropdown_cluster",
            options=[
                {'label': x, 'value': x}
                for x in []
            ],
            placeholder="Select a Cluster",
            multi=False,
            clearable=False,
            disabled=True
        ),

    ],
    style={'width': '20%', 'display': 'inline-block'}), 


    html.Div([
    

        html.P("Sites:"),
        dcc.Dropdown(
            id="dropdown_sites",
            options=[
                {'label': x, 'value': x}
                for x in []
            ],
            placeholder="Select Sites",
            multi=True,
            clearable=True,
            disabled=True
        ),

    ],
    style={'width': '29%', 'display': 'inline-block'}), 
 

    
    html.P("  "),


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

    html.Div([



        html.P("Cells:"),
        dcc.Dropdown(
            id="dropdown_cells",
            options=[
                {'label': x, 'value': x}
                for x in []
            ],
            placeholder="Select Cells",
            multi=True,
            clearable=True,
            disabled=True
        ),

    ],
    style={'width': '59%', 'display': 'inline-block'}),

#Grafico    


    html.P("  "),
    html.P("  "),
    html.Button('Generate Graph', id='button', disabled=True),
    html.P("  "),

    html.Div([
    


        dcc.Graph(id='graph', style={"height": 700}, figure={})

    ],
    style={'width': '99%',  'display': 'inline-block'}),



    html.Div([
    

        html.Button("Download CSV", id="btn_csv"),
        dcc.Download(id="download-dataframe-csv"),

    ],
    style={'width': '20%', 'display': 'inline-block'}), 


    html.Div([
    

        html.Button("Download Baseline LTE", id="btn_LTE"),
        dcc.Download(id="download-dataframe-LTE"),

    ],
    style={'width': '20%', 'display': 'inline-block'}), 


    html.Div([
    

        html.Button("Download Baseline UMTS", id="btn_UMTS"),
        dcc.Download(id="download-dataframe-UMTS"),

    ],
    style={'width': '20%', 'display': 'inline-block'}), 




    dcc.Store(id='output-data-upload'),
    html.Div(id='output-data-upload2', hidden=True),

    dcc.Store(id='selected_kpis_global'),
    dcc.Store(id='start_d', data=date.today() - timedelta(days=7)),
    dcc.Store(id='end_d', data=date.today()),

    dcc.Store(id='data_figure', data=pd.DataFrame().to_dict()),


    dcc.Store(id='df_kpis', data=df_dict),
    #dcc.Store(id='df_site_db', data=site_db_dict),

    dcc.Store(id='cur_tech', data=''),
    dcc.Store(id='cur_region', data=''),
    dcc.Store(id='cur_cluster', data=''),

    #Celdas seleccionadas para crear el grafico
    dcc.Store(id='cur_cells', data=''),
    dcc.Store(id='cur_sites', data=''),

    dcc.Interval(id='interval-date', interval=3600000*12, n_intervals=0)



        ]),


    #UMTS TAB
        dcc.Tab(label='Future Use', style=tab_style, selected_style=tab_selected_style, children=[
            

        html.H2("UNDER CONSTRUCTION!!", id="uconst", 
        style={
            'textAlign': 'center',
            'color': 'red'
        }, hidden=False)
            

        ])
 

        ])

    ])






if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050, dev_tools_hot_reload=False)

