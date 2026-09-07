
from io import StringIO
import datetime
from fastapi import FastAPI
import pandas as pd
import requests
import math
import asyncio
import aiohttp

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # puerto por defecto de Vite
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

recursosTotales = []

def iterar_recursos(conjuntos):
    recursos = []
    for conjunto in conjuntos:
        for recurso in conjunto["resources"]:
            datum = {
                "nombre_categoria": conjunto["groups"][0]["display_name"],
                "clave_categoria": conjunto["groups"][0]["name"],
                "descripcion_categoria": conjunto["groups"][0]["description"],
                "id_categoria": conjunto["groups"][0]["id"],
                "nombre_institucion": conjunto["organization"]["title"],
                "descripcion_institucion": conjunto["organization"]["description"],
                "imagen_institucion": conjunto["organization"]["image_url"],
                "nombre_paquete": conjunto["title"],
                "clave_paquete": conjunto["name"],
                "notas_paquete": conjunto["notes"],
                "etiquetas": conjunto["tags"],
                "id_paquete": conjunto["id"],
                "creator_user_id": conjunto["creator_user_id"],
                "id_recurso": recurso["id"],
                "nombre_recurso": recurso["name"],
                "descripcion_recurso": recurso["description"],
                "url_recurso": recurso["url"],
                "frecuencia_actualizacion": recurso["update_frequency"],
                "creacion_recurso": recurso["created"],
            }
            recursos.append(datum)
    return recursos

def obtener_categorias(conjuntos):
    categorias = set(list(map(lambda x: x["groups"][0]["display_name"], conjuntos)))
    return categorias

def obtener_etiquetas(conjuntos):
    etiquetas = []
    for conjunto in conjuntos:
        objeto_etiquetas = list(map(lambda x: x["name"], conjunto["tags"]))
        etiquetas += objeto_etiquetas
    return set(etiquetas)

async def fetch(session, params):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-MX,es;q=0.9",
        "Referer": "https://www.datos.gob.mx/",
    }
    url = "https://www.datos.gob.mx/api/3/action/current_package_list_with_resources"
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 200:
            result = await response.json()
            return result['result']
        else:
            return response.error


async def multiFetch():
    maxExpectedPackages = 2000
    block_size = 50
    numRequests = math.ceil(maxExpectedPackages / block_size) 
    params = list()
    conjuntos = []
    for i in range(numRequests):
        param = {"limit" : block_size, "offset" : i * block_size}
        params.append(param)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, param) for param in params]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)

    for resultado in resultados:
        conjuntos += resultado

    return conjuntos


@app.get("/api/resources")
async def fetchResources():
    conjuntos = await multiFetch()
    recursos = iterar_recursos(conjuntos)
    global recursosTotales
    recursosTotales = recursos
    categorias = obtener_categorias(conjuntos)
    etiquetas = obtener_etiquetas(conjuntos)
    return {"conjuntos": conjuntos, "recursos": recursos, "categorias": categorias, "etiquetas": etiquetas}

@app.get("/api/promedio_semanal")
def promedio_semanal():
    data = pd.DataFrame(recursosTotales)
    data['fecha_creacion'] = pd.to_datetime(data['creacion_recurso'])
    data['semana_anio'] = data['fecha_creacion'].apply(lambda x: f"{x.isocalendar()[1]}-{x.isocalendar()[0]}")
    reps = data['semana_anio'].value_counts().reset_index().rename(columns = {'count': "total_semanal"})
    reps['anio'] = reps['semana_anio'].apply(lambda x: x.split('-')[1])
    reps['semana'] = reps['semana_anio'].apply(lambda x: x.split('-')[0])
    reps = reps.sort_values(by=['anio', 'semana'])
    promedio = (reps["total_semanal"].sum() / reps.shape[0]).round(2)
    varianza = reps["total_semanal"].var().round(2)
    desviacion = reps["total_semanal"].std().round(2)
    print(promedio, varianza, desviacion)
    return {"df": reps.to_json(orient='records'), "promedio": promedio, "varianza": varianza, "desviacion": desviacion}


def formatearSoloMes(x, anio):
    meses = {"enero": "01", 
             "febrero": "02", 
             "marzo": "03", 
             "abril": "04", 
             "mayo": "05", 
             "junio": "06", 
             "julio":"07", 
             "agosto": "08", 
             "septiembre": "09", 
             "octubre": "10", 
             "noviembre": "11", 
             "diciembre": "12"}
    palabra = x.replace("\r", '').replace("\n", '').strip().lower()
    # En caso de que solo traiga el mes
    if palabra in meses.keys():
        palabra = f"01-{meses[palabra]}-{anio}"
    else:
        # Si la fecha ya es de la forma "%d-%m-%Y"
        try:
            palabra = palabra.replace("/", "-")
            palabra = datetime.datetime.strptime(x, "%d-%m-%Y")
        except:
            try:
                prueba = palabra.split('de')
                for i in range(len(prueba)):
                    prueba[i-1] = prueba[i-1].strip()
                prueba[1] = meses[prueba[1]]
                prueba = "-".join(prueba)
                palabra = datetime.datetime.strptime(prueba, "%d-%m-%Y")
            except:
                print("No se pudo parsear como fecha: ", x)
        else:
            print("No se pudo parsear de como fecha ni separando: ", x)
    return palabra

@app.get("/api/planes_apertura")
def obtenerPlanes():
    dict_columnas = {
        'poblacion_objetivo_o_sector_uso': "poblacion_objetivo_sector_uso", 
        'Periodicidad de\n publicaciÃ³n': "periodicidad_publicacion", 
        'Ã\x83ï\x86\x81rea que genera el recurso de datos': "area_genera_recurso_datos", 
        'Recurso de datos': "recurso_datos", 
        'AÂ\x81rea que genera el recurso de datos': "area_genera_recurso_datos", 
        'Observaciones': "observaciones", 
        'Formato del recurso' : "formato_recurso", 
        'fundamento': "fundamento", 
        'Fecha publicacion 2026': "fecha_publicacion", 
        'ï»¿Conjunto de datos': "conjunto_datos", 
        'Fecha publicaciÃ³n 2026': "fecha_publicacion", 
        'periodicidad_publicacion': "periodicidad_publicacion", 
        'Periodicidad de publicacioln': "periodicidad_publicacion", 
        'Descripcion del conjunto de datos': "descripcion_conjunto_datos", 
        'Ã\x81Â\x81rea que genera el recurso de datos': "area_genera_recurso_datos", 
        'Fecha de publicacion 2026': "fecha_publicacion", 
        'Ã\x81rea que genera el recurso de datos': "area_genera_recurso_datos", 
        'fecha_publicacion_2026': "fecha_publicacion", 
        'DescripciÃ³n n del recurso de datos': "descripcion_recurso_datos", 
        'ciudadanÃ\xada objetivo o sector de uso': "poblacion_objetivo_sector_uso", 
        'area_que_genera_recurso_datos': "area_genera_recurso_datos", 
        'importancia_recurso': "importancia_recurso", 
        'DescripciÃ³n del conjunto de datos': "descripcion_conjunto_datos", 
        'descripcion_recurso_datos': "descripcion_recurso_datos", 
        'Periodicidad de publicaciÃ³n': "periodicidad_publicacion", 
        'Importancia del recurso': "importancia_recurso", 
        'Ã\x83Â\x81rea que genera el recurso de datos': "area_genera_recurso_datos", 
        'Ã\x83rea que genera el recurso de datos': "area_genera_recurso_datos", 
        'Fecha de publicaciÃ³n 2026': "fecha_publicacion", 
        'formato':"formato_recurso", 
        'Fecha publicacaciÃ³n 2026': "fecha_publicacion", 
        'Area que genera el recurso de datos': "area_genera_recurso_datos", 
        'DescripciÃ³n del conjunto de datos ': "descripcion_conjunto_datos", 
        'PoblaciÃ³n objetivo o sector de uso': "poblacion_objetivo_sector_uso", 
        'Conjunto de Datos': "conjunto_datos", 
        'Periodicidad de publicacion': "periodicidad_publicacion", 
        'conjunto_datos': "conjunto_datos", 
        'Fecha publicaciÃ³n\n 2026': "fecha_publicacion", 
        'Descripcion del recurso de datos': "descripcion_recurso_datos", 
        'DescripciÃ³n del recurso de datos': "descripcion_recurso_datos", 
        'Poblacion objetivo o sector de uso': "poblacion_objetivo_sector_uso", 
        'descripcion_conjunto_datos': "descripcion_conjunto_datos", 
        'Conjunto de datos': "conjunto_datos", 
        'recurso_datos': "recurso_datos",
    }
    planes = list(filter(lambda x: x["nombre_categoria"] == 'Plan de Apertura de Datos', recursosTotales))
    listaUrls = list(map(lambda x: {'recurso': x['nombre_recurso'],'url': x['url_recurso'], 'fecha': x['creacion_recurso']}, planes))
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-MX,es;q=0.9",
        "Referer": "https://www.datos.gob.mx/",
    }
    data_frames = list()
    for url in listaUrls[0:5]:
        print("La opcion:", url)
        fecha = datetime.datetime.strptime(url['fecha'], "%Y-%m-%dT%H:%M:%S.%f")
        anio = fecha.year
        request = requests.get(url['url'], headers=headers)
        if request.status_code == 200:
            response = request.text
            df = pd.read_csv(StringIO(response), encoding='utf-8', low_memory=False)
            df = df.rename(columns = dict_columnas)
            print(df.columns)
            if df["fecha_publicacion"].dtypes == "str":
                df['fecha_alternativa'] = df['fecha_publicacion'].apply(lambda x: formatearSoloMes(x, anio))
                df['fecha_formateada'] = pd.to_datetime(df['fecha_alternativa'], format='%d-%m-%Y', errors='coerce')
            # print(df.columns)
            #data_frames.append(df)          
        else:
            print(request.status_code)
    #all = pd.concat(data_frames, ignore_index=True)
    #print(all.shape)