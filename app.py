
from azure.cosmosdb.table.tableservice import TableService

import os

import streamlit as st 

import plotly.express as px 
import pandas as pd

import numpy as np

from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

##################
def init_table_service(connection_string):
    table_service = TableService(connection_string = connection_string)
    return table_service

def search_entities(table_service,table_name, filter_expression):    
    return table_service.query_entities(table_name, filter_expression)

def get_dataframe_from_table_storage_table(st, table_service, filter_query):
    """ Create a dataframe from table storage data """
    return pd.DataFrame(get_data_from_table_storage_table(st, table_service,
                                                          filter_query))

def get_data_from_table_storage_table(st, table_service, filter_query):
    """ Retrieve data from Table Storage """
    for record in table_service.query_entities(
        st, filter=filter_query
    ):
        yield record

##################

myTenantID = "4ce25b52-c440-498c-a87d-973950aced4d"
myClientID = "2ed9aaa7-33e9-4deb-b383-a4ff97481c88"
myClientSecret = "YYS8Q~q9icJa3to8.GDVFl3VAABG~1TKkBY0Rds8"

credential = ClientSecretCredential(
    tenant_id=f"{myTenantID}",
    client_id=f"{myClientID}",
    client_secret=f"{myClientSecret}",
)

keyVaultName = "workmKV"
secretName = "myGovDataConnectionStr"   
KVUri = f"https://{keyVaultName}.vault.azure.net"    
client = SecretClient(vault_url=KVUri, credential=credential)

mySecret = client.get_secret(secretName)
connection_string = mySecret.value

table_service = init_table_service(connection_string)

sKey = "status eq 'current'" 

df = get_dataframe_from_table_storage_table('oDataABWarnings', table_service, sKey)
df['lat'] = pd.to_numeric(df.lat)
df['long'] = pd.to_numeric(df.long)
#df.drop(columns=['RowKey', 'Timestamp', 'des3', 'des4', 'des5', 'des6', 'des7', 'desCount', 'displayType', 'extent', 'future', 'isBlocked', 'point', 'startTimestamp', 'status', 'lastRunTime'])
df.drop(columns=['RowKey', 'Timestamp', 'des3', 'des4', 'des5', 'desCount', 'displayType', 'extent', 'future', 'isBlocked', 'point', 'startTimestamp', 'status', 'lastRunTime', 'etag'])

###### start st & plotly

AB = df.PartitionKey.unique()

st.set_page_config(page_title="oDataAB", page_icon="https://ftdata.blob.core.windows.net/images/logos/amc_logo_240.png")

st.write(" :bar_chart: Verkehrsinformationen D-Autobahn")

st.markdown('<style>div.block-container{padding-top:3rem;}</style>', unsafe_allow_html=True)

sidebar = st.sidebar
location_selector = sidebar.multiselect(
    "Autobahnen",
    AB,
    default=AB,
    placeholder='Autobahn'
)
st.markdown(f"# Aktuell ausgewählt {', '.join(location_selector)}")

# damit der plot immer wieder aktuallisiert wird
plot_spot = st.empty()

sLoc = np.array(location_selector)
df_filter = df[df['PartitionKey'].isin(sLoc)]

fig = px.scatter_mapbox(df_filter,
                        lon = df_filter['long'],
                        lat = df_filter['lat'],
                        zoom = 5,
                        width = 640,
                        height = 640,
                        title = 'Warnungen',
                        hover_name = df_filter['title'],
                        hover_data = {
                            "lat": False,
                            "long": False,
                            "PartitionKey": False
                        },
                        custom_data = [
                            'title',
                            'subtitle',
                            'des1',
                            'des2',
                            'des3',
                            'des4',
                            'des5',
                            'des6'
                        ],
                        labels = {
                            'PartitionKey':'Autobahnen'
                        }
                        )

fig.update_traces(hovertemplate='%{customdata[0]} <br>%{customdata[1]} <br>%{customdata[2]} <br>%{customdata[3]} <br>%{customdata[4]} <br>%{customdata[5]} <br>%{customdata[6]} <br>%{customdata[7]}')
fig.update_layout(hovermode="x")
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})

fig.update_mapboxes(center_lat=51)
fig.update_mapboxes(center_lon=10.5)

fig.update_layout(hoverlabel = dict(
    font_size = 10,
    font_family = "Arial"
))

with plot_spot:
    st.plotly_chart(fig,
                    theme="streamlit"
                    )

