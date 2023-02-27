import dash
import flask

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = flask.Flask(__name__)

app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets, update_title='Calculating, Please Wait...',
	title='NOKIA Viewer')
#server = app.server