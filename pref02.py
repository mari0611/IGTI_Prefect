from datetime import datetime, timedelta
import pendulum
import prefect
from prefect import task, Flow
from prefect.schedules import CronSchedule
import pandas as pd 
from io import BytesIO
import zipfile
import requests
import sqlalchemy
import pyodbc

schedule = CronSchedule(
    cron = "*/10 * * * *",
    start_date=pendulum.datetime(2020, 12, 1, 13, 45, tz='America/Sao_Paulo')
    
)

@task
def get_raw_data():
    url = "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip",
    filebytes = BytesIO(requests.get(url).content)

    #extrair conteudo do zip
    myzip = zipfile.ZipeFile(filebytes)
    myzip.extractall()
    path = './microdados_enade_2019/2019/3.DADOS/'
    return path

@task
def aplica_filtros(path):
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 
            'QE_I01', 'QE_I02']
    enade = pd.read_csv(path + 'microdados_enade_2019.txt', sep=';', decimal=',', usecols='cols')
    enade = enade.loc[
        (enade.NU_IDADE > 20) & 
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    return enade

@task
def constroi_idade_centralizada(df):
    idade = df[['NU_IDADE']]
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    return idade[['idadecent']]

@task
def constroi_idade_cent_quad(df):
    idadecent = df.copy()
    idadecent['idade2'] = idadecent.idadecent **2
    return idadecent[['idade2']]


@task
def join_data(df, idadecent, idadequadrado, estcivil, cor, escopai, escomae, renda):
    final = pd.concat([
        df, idadecent, idadequadrado, estcivil, cor, escopai,
        escomae, renda
    ], axis=1)

    final = final[['CO_GRUPO', 'TP_SEXO', 'idadecent', 'idade2', 'estcivel',
                    'cor', 'escopai', 'escomae', 'renda']]

    logger = prefect.context.get('logger')
    logger.info(final.head().to_json())
    final.to_csv('enade_tratado.csv', index=False)  

@task
def escreve_dw(df):
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc://"
    )
    df.to_sql("tratado", con=engine, index=False, if_exists='append', method='multi')


## Encadeamento
with Flow('Enade', schedule) as flow:
    path = get_raw_data()
    filtro = aplica_filtros(path)
    idadecent = constroi_idade_centralizada(filtro)
    idadequadrado = constroi_idade_cent_quad(idadecent)


    j = join_data(filtro, idadecent, idadequadrado)

    w = escreve_dw(j)

flow.register(project_name='IGTI', idempotency_key=flow.serialized_hash() )
flow.run_agent(token="")  ## definir como variáveis de ambiente

