# Libraries
import facebook
import requests
import json
import datetime
from datetime import timedelta, date
import pandas as pd
import time
import joinAnalytics
import calendar
from metrics import Metrics
from sqlalchemy import create_engine


# Constant to set the URL
engine = create_engine(
    "postgresql://user:pass@host/database")
token = 'xxxxxxxxxx'
graph = facebook.GraphAPI(access_token=token, version=3.1)
page_info = graph.get_object('me')
page_id = page_info['id']
base = 'https://graph.facebook.com/v3.2'

# Intance of a class that contains the metrics to retrieve
metrics_class = Metrics()
metricas = metrics_class.returnMetricas()


def daterange(start_date, end_date):
	"""Receive the dates when I want to know the insights \n
    	and it returns a generator itself  
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def setDate():
    """Set the days in which I will make the request to the Facebook API \n
         Returns:
             since: Start date
             until: End date
    """
    hoy = datetime.datetime.now()
    date_str = hoy.strftime("%m/%d/%Y")
    hoy_dt = datetime.datetime.strptime(date_str, '%m/%d/%Y')
    since = hoy_dt - timedelta(3)
    until = hoy_dt - timedelta(2)

    return since, until


def getInsights(insights):
    """Receive all the metrics and generate a string \n
        so it can be used by the request to the API"""
    metricas = ', '.join(insights)
    metricas = metricas.replace(' ', '')
    return metricas


def ordenoMetricas(metrica):
    """Check if the metric is a dictionary containing \n
      other metrics and I parse it, it returns ordered"""
    dict_metricas = {}
    for metric in metrica:
        nombre_metrica = metric['name']
        valor = metric['values'][0]['value']
        if type(valor) is dict:
            for key, value in valor.items():
                nombre_metrica = key
                valor = value
                dict_metricas[nombre_metrica] = valor
        else:
            dict_metricas[nombre_metrica] = valor

    return dict_metricas


def conectAnalytics(fecha, url, tipo):
    if tipo == 'link':
        dict = joinAnalytics.main(fecha, url)
        return dict
    else:
        dict = {'PV_dia_1': '0', 'PV_dia_2': '0',
                'PV_dia_3': '0', 'PV_dia_4': '0'}
        return dict


def parsingTime(fecha_publicado):
    """Parse the published date for \n
	 	that returns the one that I am going to use in getPageviews ()
    """
    list1 = fecha_publicado.split('T')
    fecha = list1[0]
    list2 = list1[1].split('+')
    hora = list2[0]
    date = datetime.datetime.strptime(fecha, '%Y-%m-%d')
    dia = calendar.day_name[date.weekday()]

    if dia == 'Monday':
        dia = 'Lunes'
    elif dia == 'Tuesday':
        dia = 'Martes'
    elif dia == 'Wednesday':
        dia = 'Miercoles'
    elif dia == 'Thursday':
        dia = 'Jueves'
    elif dia == 'Friday':
        dia = 'Viernes'
    elif dia == 'Saturday':
        dia = 'Sabado'
    elif dia == 'Sunday':
        dia = 'Domingo'

    return fecha, hora, dia


def parsingURL(link_del_post):
    """Parse the URL after the .com \n
        Returns:
            URL: "/Seccion/nota.html"
    """
    partes = {}
    link_del_post_splitted = link_del_post.split('//')
    link = link_del_post_splitted[1].split('/')
    partes['link'] = link[0]

    if 'doers' in link_del_post:
        url = link_del_post.split('doers.video')
        link1 = url[1].split('.html')
        link_doers = link1[0]
        link_doers_splitted = link_doers.split('/')
        partes['seccion'] = link_doers_splitted[1]
        partes['url'] = link_doers
    elif 'bioguia' in link_del_post and 'bioguia.doers' not in link_del_post:
        url = link_del_post.split('.com')
        link1 = url[1].split('.html')
        link_bioguia = link1[0]
        link_bioguia_splitted = link_bioguia.split('/')
        partes['seccion'] = link_bioguia_splitted[1]
        partes['url'] = link_bioguia
    else:
        if '.gl' in link_del_post:
            partes['seccion'] = 'Sin Seccion'
            partes['url'] = 'Short link google'
        else:
            url = link_del_post.split('.com')
            link1 = url[1].split('.html')
            link_splitted = link1[0]
            link_bioguia_splitted = link_splitted.split('/')
            partes['seccion'] = link_bioguia_splitted[1]
            partes['url'] = link_splitted
    return partes


def checkCopy(post):
    """Check that the name and copy have something
    """
    if 'message' in post:
        copy_to_return = post['message']
        return copy_to_return
    else:
        copy_to_return = 'Sin Copy'
        return copy_to_return

    if 'name' in post:
        name_to_return = post['name']
        return name_to_return
    else:
        name_to_return = 'Sin Name'
        return name_to_return


def getPost(since, until, metricas):
    """He is in charge of making the request to the Facebook API \n
		Returns: 
			DataFrame: DF of the date that was passed to him
    """
    print(since, type(since))
    print(until, type(until))

    node = '/' + page_id + \
        '?fields=posts.limit(100).since(' + str(since) + ').until(' + str(until) + '){id,link,name,created_time,message,type,permalink_url,' + \
        'insights.metric(' + getInsights(metricas) + \
        ').period(lifetime)}'
    url = base + node
    parameters = {'access_token': token, 'limit': '100'}
    object1 = requests.get(url, params=parameters).text.encode('utf-8')
    data = json.loads(object1)
    data = data['posts']['data']
    dict_posts = {}
    list_dicts = []
    for post in data:
        fecha = parsingTime(post['created_time'])[0]
        hora = parsingTime(post['created_time'])[1]
        dia = parsingTime(post['created_time'])[2]
        print(post['id'], post['link'])
        full_link = parsingURL(post['link'])
        url = full_link['url']
        dict_posts['sitio'] = full_link['link']
        dict_posts['seccion'] = full_link['seccion']
        dict_pageviews = conectAnalytics(
            fecha, url, post['type'])
        pv_dia_1 = dict_pageviews['PV_dia_1']
        pv_dia_2 = dict_pageviews['PV_dia_2']
        pv_dia_3 = dict_pageviews['PV_dia_3']
        pv_dia_4 = dict_pageviews['PV_dia_4']
        dict_posts['id'] = post['id']
        dict_posts['publicado'] = fecha+'__'+hora
        dict_posts['fecha'] = fecha
        dict_posts['hora'] = hora
        dict_posts['dia'] = dia
        dict_posts['link_al_post'] = post['permalink_url']
        dict_posts['link_del_post'] = post['link']
        dict_posts['copy'] = checkCopy(post)
        dict_posts['name'] = checkCopy(post)
        dict_posts['tipo'] = post['type']
        dict_posts['pv_dia1'] = pv_dia_1
        dict_posts['pv_dia2'] = pv_dia_2
        dict_posts['pv_dia3'] = pv_dia_3
        dict_posts['pv_dia4'] = pv_dia_4
        metricas = post['insights']['data']
        dict_metrics = ordenoMetricas(metricas)
        z = {**dict_posts, **dict_metrics}
        list_dicts.append(z.copy())
    df0 = pd.DataFrame(list_dicts)
    list_index_df = metrics_class.returnReIndexDf()
    df0 = df0.reindex(list_index_df, axis=1)
    print(df0)
    return df0


def getDFofDate():
    df_master = pd.DataFrame()
    start_date = setDate()[0]
    end_date = setDate()[1]
    for single_date in daterange(start_date, end_date):
        since1 = single_date.strftime("%m/%d/%Y")
        since = datetime.datetime.strptime(since1, "%m/%d/%Y")
        since = since.replace(minute=00, hour=00, second=00)
        until = single_date + timedelta(days=1)
        until1 = until.strftime("%m/%d/%Y")
        until = datetime.datetime.strptime(until1, "%m/%d/%Y")
        until = until.replace(minute=00, hour=00, second=00)
        df_of_date = getPost(since, until, metricas)
        df_master = df_master.append(df_of_date, ignore_index=True)

    return df_master


# Generates the DataFrame
master = getDFofDate()

# Get calculated Metrics
calculated = metrics_class.returnCalculatedMetrics(master)

# Gives rounded numbers fon the metrics
result = calculated.round(4)
result.fillna(0, inplace=True)
# Sort everytihing by date and hour
result.sort_values(['fecha', 'hora'], inplace=True)
print(result)

# Now it goes everything to PostgreSQL Database so then Data Studio can retrieve it
result.to_sql('fb_insights', engine, schema='public', if_exists='append')