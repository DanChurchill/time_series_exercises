import numpy as np
import pandas as pd
import requests
import os

def get_items(url='https://python.zgulde.net/api/v1/items?page=1'):
    '''
    function to retrieve item data from website.
    accepts an url, returns a dataframe with items information
    '''
    max_page = requests.get(url).json()['payload']['max_page'] + 1
    output = []
    for i in range(1,max_page):
        url = url[:-1] + str(i)
        if i == 1:
            output = pd.DataFrame(requests.get(url).json()['payload']['items'])
        else:
            output = pd.concat([output, pd.DataFrame(requests.get(url).json()['payload']['items'])], 
                               ignore_index=True)
    return output




def get_stores(url='https://python.zgulde.net/api/v1/stores'):
    '''
    function to retrieve store data from website.
    accepts an url, returns a dataframe with store information
    '''
    return pd.DataFrame(requests.get(url).json()['payload']['stores'])


def get_sales(url = 'https://python.zgulde.net/api/v1/sales?page='):
    '''
    function to retrieve sales data from website.
    accepts an url, returns a dataframe with sales information
    '''
    max_page = requests.get(url+'1').json()['payload']['max_page'] + 1
    output = []
    for i in range(1,max_page):
        new_url = url + str(i)
        if i == 1:
            output = pd.DataFrame(requests.get(new_url).json()['payload']['sales'])
        else:
            output = pd.concat([output, pd.DataFrame(requests.get(new_url).json()['payload']['sales'])], 
                               ignore_index=True)
    return output

def combine(sales, stores, items):
    '''
    function to combine three dataframes of store data
    accepts 3 dataframes and returns them joined into one dataframe
    
    '''
    combo = sales.merge(items, left_on='item', right_on='item_id')
    return combo.merge(stores, left_on='store', right_on='store_id')


def superstore():
    '''
    Retrieve locally cached data .csv file for the superstore dataset
    If no locally cached file is present then retrieve sales, items, 
    and store dataframes from the web, merge them, cache the file as a csv, 
    and return the merged dataframe
    '''
    # if file is available locally, read it
    filename = 'sales.csv'
    if os.path.isfile(filename):
        return pd.read_csv(filename)

    else:
        # retrieve and combine data
        sales = get_sales()
        stores = get_stores()
        items = get_items()
        
        df = combine(sales, stores, items)

        # Write that dataframe to disk for later. This cached file will prevent repeated large queries to the database server.
        df.to_csv(filename, index=False)
    
    return df




def get_ops():
    '''
    function to retrieve Open Power Systems Data for Germany
    returns a dataframe
    '''
    return pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')



