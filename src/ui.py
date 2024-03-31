import dash
from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
import dash_daq as daq
import flask
from routes.kaizenbrain.helpers_db import run_sql, get_engine
from routes.kaizenbrain.helpers_queries import queries

server = flask.Flask(__name__)

@server.route("/")
def home():
    return "Hello, Flask!"

#####################################################
# Part 2: Basic app information
#####################################################
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP], server=server, routes_pathname_prefix="/positions/")
app.title = "Stock Analysis Dashboard"
CONTENT_STYLE = {"margin-left": "2rem", "margin-right": "2rem", "padding": "2rem 1rem"}

engine = get_engine()
with engine.begin() as conn:
  balance = run_sql(conn, queries['balance']).mappings().first()
  df_positions = pd.DataFrame.from_dict(run_sql(conn, queries['positions_status']).mappings().all())
  df_crossing_levels = pd.DataFrame.from_dict(run_sql(conn, queries['crossing_levels']).mappings().all())

#####################################################
# Part 4: App layout
#####################################################
color_negative      = '#FF5E5E'
color_positive      = '#B0C5A4'
classNameSixColumns = 'columns'
div_dropdown_symbol = html.Div(dcc.Dropdown(
    id='symbol-dropdown', value=df_positions.symbol.to_list(), clearable=False, multi=True,
    options=[{'label': x, 'value': x} for x in sorted(df_positions.symbol.unique())]
), className=classNameSixColumns, style={"width": "50%"}, )

div_dropdown_field = html.Div(dcc.Dropdown(
    id='field-dropdown', value='symbol', clearable=False,
    options=[{'label': x, 'value': x} for x in [i for j, i in enumerate(df_positions.columns) if j not in ['symbol']]]
), className=classNameSixColumns, style={"width": "25%"}, )

div_led_gains_today = html.Div(daq.LEDDisplay(
  id="led-gains-today",
  value='{:.3f}'.format(balance["profits_hist_perc"] * 100),
  color= color_positive if balance["profits_hist_perc"] > 0 else color_negative,
  size=50,
), className=classNameSixColumns, style={"width": "25%"}, )
div_led_gains_1d_ago = html.Div(daq.LEDDisplay(
  id="led-gains-1d_ago",
  value='{:.3f}'.format(balance["profits_1d_ago_perc"] * 100),
  color= color_positive if balance["profits_1d_ago_perc"] > 0 else color_negative,
  size=50,
), className='six columns', style={"width": "25%"}, )
div_led_gains_value = html.Div(daq.LEDDisplay(
  id="led-gains-history",
  value='{:.2f}'.format(balance["profit_hist_value"]),
  color= color_positive if balance["profit_hist_value"] > 0 else color_negative,
  size=50,
), className=classNameSixColumns, style={"width": "50%"}, )

app.layout = html.Div([
    html.Div([div_led_gains_1d_ago, div_led_gains_today, div_led_gains_value], className='row'),
    html.Div([html.Div([div_dropdown_symbol, div_dropdown_field], className='row'), ], className='custom-dropdown'),
    html.Div(html.Div(id='table-container_1'), style={'marginBottom': '15px', 'marginTop': '0px'}),
    html.Div(html.Div(id='table-container_2'), style={'marginBottom': '15px', 'marginTop': '0px'}),
  ], style=CONTENT_STYLE)

money                   = dash_table.FormatTemplate.money(2)
percentage              = dash_table.FormatTemplate.percentage(2)
columns_crossing_levels = [
   dict(id='symbol',    name='Symbol'),
   dict(id='ago',       name='Time Ago'),
   dict(id='level',     name='Level',           type='numeric', format=percentage),
   dict(id='direction', name='Trend'),
   dict(id='crossing',  name='Type Crossing'),
   dict(id='dist',      name='Distance',        type='numeric', format=percentage),
   dict(id='movement',  name='Movement',        type='numeric', format=percentage)
]
cols_colors_crossing_levels = ['movement']


@app.callback([Output('table-container_1', 'children')],
              [Input(component_id='symbol-dropdown', component_property='value'),
               Input(component_id='field-dropdown', component_property='value')])
def display_value(symbol_chosen, field_chosen):
  return (dash_table.DataTable(columns=columns_crossing_levels,
                               id='datatable-crossing-levels',
                               style_data_conditional=(
                                    # [{ 'if': { 'row_index': 'even', 'filter': 'row_index > num(2)' }, 'backgroundColor': '#EBEDEF' }] + 
                                    [{ 'if': { 'filter_query': '{{{}}} > 0'.format(col, value), 'column_id': col }, 'color': 'green'
                                     } for (col, value) in df_crossing_levels[cols_colors_crossing_levels].quantile(0.1).items()] +
                                    [{ 'if': { 'filter_query': '{{{}}} < 0'.format(col, value), 'column_id': col }, 'color': 'tomato'
                                     } for (col, value) in df_crossing_levels[cols_colors_crossing_levels].items()] 
                                ),
                               data=df_crossing_levels.sort_values(by=[field_chosen]).to_dict('records'),
                               export_format="csv",
                               style_as_list_view=True,
                               fill_width=True,
                               sort_action='native',
                               sort_mode='single',
                               style_cell={'font-size': '12px'},
                               style_header={'backgroundColor': 'black', 'color': 'white', },
                               fixed_rows={'headers': True, 'data': 0},
                               ),)

columns_positions       = [
    dict(id='symbol', name='Symbol'),
    dict(id='amount', name='Amount', type='numeric', format=money),
    
    dict(id='quote_purchase', name='Purchase ($)', type='numeric', format=money),
    dict(id='quote_last_quote', name='Last Quote ($)', type='numeric', format=money),
    dict(id='ticks_yesterday', name='Yesterday ($)', type='numeric', format=money),
    
    dict(id='profit_historical', name='Profit ($)', type='numeric', format=money),
    dict(id='profits_last_day', name='Last Day ($)', type='numeric', format=money),
    
    dict(id='profit_historical_perc', name='Profit (%)', type='numeric', format=percentage),
    dict(id='profits_last_day_perc', name='Last Day (%)', type='numeric', format=percentage),
    dict(id='participation', name='Weight', type='numeric', format=percentage)
]
cols_colors_positions = ['profit_historical','profit_historical_perc','profits_last_day','profits_last_day_perc']


@app.callback(
    Output('table-container_2', 'children'),
    [Input(component_id='symbol-dropdown', component_property='value'),
     Input(component_id='field-dropdown', component_property='value')]
)
def display_value(symbol_chosen, field_chosen):
  return (dash_table.DataTable(columns=columns_positions,
                               id='datatable-positions',
                               style_data_conditional=(
                                    [
                                        # { 'if': { 'row_index': 'even', 'filter': 'row_index > num(2)' }, 'backgroundColor': '#EBEDEF' },
                                    ] + 
                                    [
                                        { 'if': { 'filter_query': '{{{}}} > 0'.format(col, value), 'column_id': col }, 'color': 'green'
                                        } for (col, value) in df_positions[cols_colors_positions].quantile(0.1).items()
                                    ] +
                                    [
                                        { 'if': { 'filter_query': '{{{}}} < 0'.format(col, value), 'column_id': col }, 'color': 'tomato'
                                        } for (col, value) in df_positions[cols_colors_positions].items()
                                    ] 
                                ),
                               data=df_positions.sort_values(by=[field_chosen]).to_dict('records'),
                               export_format="csv",
                               style_as_list_view=True,
                               fill_width=True,
                               sort_action='native',
                               sort_mode='single',
                               style_cell={'font-size': '12px'},
                               style_header={'backgroundColor': 'black', 'color': 'white', },
                               fixed_rows={'headers': True, 'data': 0},
                               ),)


if __name__ == "__main__":
    app.run_server(debug=True)