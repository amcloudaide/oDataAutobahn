
from azure.cosmosdb.table.tableservice import TableService
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

import os
import streamlit as st
import plotly.express as px 
import pandas as pd
import numpy as np

######## helper ##########

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

myTenantID = os.getenv('AZURE_TENANT_ID')
myClientID = os.getenv('AZURE_CLIENT_ID')
myClientSecret = os.getenv('AZURE_CLIENT_SECRET')

credential = ClientSecretCredential(
    tenant_id=f"{myTenantID}",
    client_id=f"{myClientID}",
    client_secret=f"{myClientSecret}",
)

keyVaultName = os.getenv('KEY_VAULT_NAME')
secretName = os.getenv('KEY_VAULT_SECRET')  
KVUri = f"https://{keyVaultName}.vault.azure.net"    
client = SecretClient(vault_url=KVUri, credential=credential)

mySecret = client.get_secret(secretName)
connection_string = mySecret.value

table_service = init_table_service(connection_string)

sKey = "status eq 'current'" 

df_r = get_dataframe_from_table_storage_table('oDataABRoadworks', table_service, sKey)
df_r['lat'] = pd.to_numeric(df_r.lat)
df_r['long'] = pd.to_numeric(df_r.long)

df_c = get_dataframe_from_table_storage_table('oDataABClosure', table_service, sKey)
df_c['lat'] = pd.to_numeric(df_c.lat)
df_c['long'] = pd.to_numeric(df_c.long)

df_w = get_dataframe_from_table_storage_table('oDataABWarnings', table_service, sKey)
df_w['lat'] = pd.to_numeric(df_w.lat)
df_w['long'] = pd.to_numeric(df_w.long)

###### start st & plotly

st.set_page_config(page_title="oDataAB", page_icon="https://ftdata.blob.core.windows.net/images/logos/amc_logo_240.png")
st.write(" :bar_chart: Verkehrsinformationen D-Autobahn")
st.markdown('<style>div.block-container{padding-top:3rem;}</style>', unsafe_allow_html=True)

sidebar = st.sidebar

stoerung_selector = sidebar.radio(
    "Störungen",
    ['Baustellen', 'Sperrungen', 'Warnungen'],
    index=2
)

if stoerung_selector == 'Warnungen':
    #st.write('a')
    AB = df_w.PartitionKey.unique()
    df_filter = df_w[df_w['PartitionKey'].isin(AB)]
    #st.write(AB)
if stoerung_selector == 'Sperrungen':
    #st.write('b')
    AB = df_c.PartitionKey.unique()
    df_filter = df_c[df_c['PartitionKey'].isin(AB)]
    #st.write(AB)
if stoerung_selector == 'Baustellen':
    #st.write('c')
    AB = df_r.PartitionKey.unique()
    df_filter = df_r[df_r['PartitionKey'].isin(AB)]
    #st.write(AB)

#AB = df.PartitionKey.unique()

location_selector = sidebar.multiselect(
    "Autobahnen",
    AB,
    default=AB,
    placeholder='Autobahn'
)
sLoc = np.array(location_selector)
df_sLoc = df_filter[df_filter['PartitionKey'].isin(sLoc)]

st.markdown(f"Aktuell ausgewählt {', '.join(location_selector)}")

# damit der plot immer wieder aktuallisiert wird
plot_spot = st.empty()

fig = px.scatter_mapbox(df_sLoc,
                        lon = df_sLoc['long'],
                        lat = df_sLoc['lat'],
                        zoom = 5,
                        width = 640,
                        height = 640,
                        title = stoerung_selector,
                        hover_name = df_sLoc['title'],
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
