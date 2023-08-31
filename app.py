
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

df_r = get_dataframe_from_table_storage_table('oDataABRoadworks', table_service, sKey)
df_r['lat'] = pd.to_numeric(df_r.lat)
df_r['long'] = pd.to_numeric(df_r.long)

df_c = get_dataframe_from_table_storage_table('oDataABClosure', table_service, sKey)
df_c['lat'] = pd.to_numeric(df_c.lat)
df_c['long'] = pd.to_numeric(df_c.long)

df_w = get_dataframe_from_table_storage_table('oDataABWarnings', table_service, sKey)
df_w['lat'] = pd.to_numeric(df_w.lat)
df_w['long'] = pd.to_numeric(df_w.long)

df_p = get_dataframe_from_table_storage_table('oDataABParking', table_service, sKey)
df_p['lat'] = pd.to_numeric(df_p.lat)
df_p['long'] = pd.to_numeric(df_p.long)

df_e = get_dataframe_from_table_storage_table('oDataABEChargingStation', table_service, sKey)
df_e['lat'] = pd.to_numeric(df_e.lat)
df_e['long'] = pd.to_numeric(df_e.long)

###### start st & plotly

st.set_page_config(page_title="oDataAB", page_icon="https://ftdata.blob.core.windows.net/images/logos/amc_logo_240.png")
st.write(" :bar_chart: Verkehrsinformationen D-Autobahn")
st.markdown('<style>div.block-container{padding-top:3rem;}</style>', unsafe_allow_html=True)

sidebar = st.sidebar

stoerung_selector = sidebar.radio(
    "Störungen",
    ['Baustellen', 'Sperrungen', 'Warnungen', 'Parkplätze', 'Ladestationen'],
    index=2
)

if stoerung_selector == 'Warnungen':
    #st.write('a')
    AB = df_w.PartitionKey.unique()
    df_filter = df_w[df_w['PartitionKey'].isin(AB)]
    cd = [
            'title',
            'subtitle',
            'des1',
            'des2',
            'des3',
            'des4',
            'des5',
            'des6'
        ]
    ht = '%{customdata[0]} <br>%{customdata[1]} <br>%{customdata[2]} <br>%{customdata[3]} <br>%{customdata[4]} <br>%{customdata[5]} <br>%{customdata[6]} <br>%{customdata[7]}'
    #st.write(AB)
if stoerung_selector == 'Sperrungen':
    #st.write('b')
    AB = df_c.PartitionKey.unique()
    df_filter = df_c[df_c['PartitionKey'].isin(AB)]
    cd = [
            'title',
            'subtitle',
            'des1',
            'des2',
            'des3',
            'des4',
            'des5',
            'des6'
        ]
    ht = '%{customdata[0]} <br>%{customdata[1]} <br>%{customdata[2]} <br>%{customdata[3]} <br>%{customdata[4]} <br>%{customdata[5]} <br>%{customdata[6]} <br>%{customdata[7]}'
    #st.write(AB)
if stoerung_selector == 'Baustellen':
    #st.write('c')
    AB = df_r.PartitionKey.unique()
    df_filter = df_r[df_r['PartitionKey'].isin(AB)]
    cd = [
            'title',
            'subtitle',
            'des1',
            'des2',
            'des3',
            'des4',
            'des5',
            'des6'
        ]
    ht = '%{customdata[0]} <br>%{customdata[1]} <br>%{customdata[2]} <br>%{customdata[3]} <br>%{customdata[4]} <br>%{customdata[5]} <br>%{customdata[6]} <br>%{customdata[7]}'
    #st.write(AB)

if stoerung_selector == 'Parkplätze':
    #st.write('c')
    AB = df_p.PartitionKey.unique()
    df_filter = df_p[df_p['PartitionKey'].isin(AB)]
    cd = [
            df_filter['title'],
            df_filter['subtitle'],
            df_filter['LKW_No'],
            df_filter['PKW_No'],
            df_filter['pFeatureList']
        ]
    ht = '%{customdata[0]} <br>%{customdata[1]} <br>LKW: %{customdata[2]} <br>PKW: %{customdata[3]} <br>Ausstattung: %{customdata[4]}'
    #st.write(AB)

if stoerung_selector == 'Ladestationen':
    #st.write('c')
    AB = df_e.PartitionKey.unique()
    df_filter = df_e[df_e['PartitionKey'].isin(AB)]
    cd = [
            df_filter['title'],
            df_filter['subtitle'],
            df_filter['des1'],
            df_filter['des2'],
            df_filter['des3'],
        ]
    ht = '%{customdata[0]} <br>%{customdata[1]} <br>%{customdata[2]} <br>%{customdata[3]} <br>%{customdata[4]}'
    #st.write(AB)


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
                        width = 700,
                        height = 700,
                        title = stoerung_selector,
                        hover_name = df_sLoc['title'],
                        hover_data = {
                            "lat": False,
                            "long": False,
                            "PartitionKey": False
                        },
                        custom_data = cd,
                        )

fig.update_traces(hovertemplate=ht)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})

fig.update_mapboxes(center_lat=51)
fig.update_mapboxes(center_lon=9)

fig.update_layout(hoverlabel = dict(
    font_size = 10,
    font_family = "Arial"
))

with plot_spot:
    st.plotly_chart(fig,
                    theme="streamlit"
                    )
