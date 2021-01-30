import pandas as pd
from functools import reduce
import requests
from sqlalchemy import create_engine
import json
from bs4 import BeautifulSoup
import re
import time

# Database Connection:

def get_engine_db(path):
    print(f'Connecting!')
    engine = create_engine(f'sqlite:///{path}')
    df_personal = pd.read_sql_table(table_name='personal_info', con=engine)
    df_career = pd.read_sql_table(table_name='career_info', con=engine)
    df_country = pd.read_sql_table(table_name='country_info', con=engine)
    df_poll = pd.read_sql_table(table_name='poll_info', con=engine)
    df_database_merge = reduce(lambda left_table, right_table: pd.merge(left_table, right_table, on='uuid'),
                               [df_personal, df_career, df_country, df_poll])

    df_database_merge.to_csv('./data/raw/database_merge.csv', index=False)

    print('Go grab a coffee sir/madame ;)\n', end='')
    time.sleep(2)
    print('Not yet my friend, be patient\n', end='')
    time.sleep(2)
    print('Not infartitos, please. It is gonna be ok. Lets talk about the EU.\n\n\n', end='')
    time.sleep(4)
    print("""La Unión Europea nació con el anhelo de acabar con los frecuentes y cruentos conflictos
        entre vecinos que habían culminado en la Segunda Guerra Mundial.
        En los años 50, la Comunidad Europea del Carbón y del Acero es el primer paso de una
        unión económica y política de los países europeos para lograr una paz duradera.
        Sus seis fundadores son Alemania, Bélgica, Francia, Italia, Luxemburgo y los Países Bajos.
        Ese período se caracteriza por la guerra fría entre el este y el oeste. Las protestas contra el 
        régimen comunista en Hungría son aplastadas por los tanques soviéticos en 1956.
        En 1957 se firma el Tratado de Roma, por el que se constituye la Comunidad Económica Europea (CEE) o "mercado común""", end='')
    time.sleep(1)
    print('\n\n\nNot yet my friend, be patient\n', end='')

    print('Exporting database_merge.csv!')


    return df_database_merge


# API Connection:

def get_api_jobs(jobs):
    jobs_id = list(pd.unique(jobs['normalized_job_code']))
    jobs_id.remove(None)
    request_list = [requests.get(f'http://api.dataatwork.org/v1/jobs/{job_id}').json() for job_id in jobs_id]
    request_json = json.dumps(request_list)
    df_jobs_title = pd.read_json(request_json)
    print('Exporting API info into jobs_title.csv!')
    df_jobs_title.to_csv('./data/raw/jobs_title.csv', index=False)
    print(f'Done!')

    return df_jobs_title


# Web Scraping:
def get_web_scraping(countries):
    url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'
    html = requests.get(url).content
    soup = BeautifulSoup(html, 'lxml')
    table = \
    soup.find_all('div', {'class': 'col-lg-12 col-md-12 col-sm-12 col-xs-12 content-col content article-content'})[0]
    rows = table.find_all('td')
    rows_parsed = [row.text for row in rows]
    def_rows = [re.sub('^\s|[()]|\n|(?:\[\\d])|\*\s|[*]', '', i) for i in rows_parsed]
    def_rows = [re.sub('UK', 'GB', i) for i in def_rows]
    def_rows = [re.sub('EL', 'GR', i) for i in def_rows]
    def_rows = [x for x in def_rows if x]
    country_relationship = [def_rows[x:x + 2] for x in range(0, len(def_rows), 2)]
    df_country_relationship = pd.DataFrame(country_relationship)
    df_country_relationship_colnames = ['country', 'country_code']
    df_country_relationship.columns = df_country_relationship_colnames
    print('Exporting scrap_countries.csv!')                       #MagicReSub credits to a python programmer

    df_country_relationship.to_csv('./data/raw/scrap_countries.csv')
    print(f'Done!')

    return df_country_relationship


# Merge all clean data obtained:

def dataframes_to_merge(data_db, data_api, data_scraping):
    merged_personal_job_info = pd.merge(data_db, data_api, left_on='normalized_job_code', right_on='uuid', how='outer',
                                        sort=False, suffixes=('', '_y'))
    # Merge db and API with Web Scraped info
    merged_ws_job_info = pd.merge(merged_personal_job_info, data_scraping, left_on='country_code',
                                  right_on='country_code',
                                  how='inner', sort=False, suffixes=('', '_y'))

    merged_ws_job_info = merged_ws_job_info.rename(columns={'question_bbi_2016wave4_basicincome_awareness': 'awareness',
                                                            'question_bbi_2016wave4_basicincome_vote': 'vote',
                                                            'question_bbi_2016wave4_basicincome_effect': 'effect',
                                                            'question_bbi_2016wave4_basicincome_argumentsfor': 'arguments_for',
                                                            'question_bbi_2016wave4_basicincome_argumentsagainst': 'arguments_against'})

    return merged_ws_job_info


def acquire(path):
    print(f'Reading data from {path}...')
    table = get_engine_db(path)
    api = get_api_jobs(table)
    wscraping = get_web_scraping(api)
    df_merge = dataframes_to_merge(table, api, wscraping)

    return df_merge
