
from io import BytesIO
import datetime
from fastapi import FastAPI, HTTPException
import pandas as pd
import requests
import math
import asyncio
import aiohttp
import itertools
import time

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

_cache = {}
TTL_SEGUNDOS = 300  # 5 minutos, ajustalo según qué tan seguido cambian tus datos

async def fetch_con_cache(session, param):
    key = str(param)  # ajustá esto si param es un dict, como vimos arriba
    ahora = time.time()

    # 1. ¿Ya tengo este dato guardado y sigue "fresco"?
    if key in _cache:
        resultado, timestamp = _cache[key]
        if ahora - timestamp < TTL_SEGUNDOS:
            return resultado  # <- no hacemos ningún request, devolvemos lo guardado

    # 2. No estaba, o estaba vencido: pedimos de verdad
    resultado = await fetch(session, param)

    # 3. Guardamos el resultado nuevo + el momento actual
    _cache[key] = (resultado, ahora)

    return resultado

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
        tasks = [fetch_con_cache(session, param) for param in params]
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

@app.get("/api/recursos_x_categoria")
async def contarRecursosPorCategoria():
    data = pd.DataFrame(recursosTotales)
    data['mes'] = pd.to_datetime(data['creacion_recurso']).dt.month.apply(str)
    data['anio'] = pd.to_datetime(data['creacion_recurso']).dt.year.apply(str)
    data['fecha'] = data['mes'].str.cat(data['anio'], sep="-")
    datum = data[['nombre_categoria', 'fecha']].groupby(['nombre_categoria', 'fecha']).size()
    datum = datum.reset_index().rename(columns = {'count': 'reps'})
    print(datum.head(10))
    return {"datum" : datum.to_json(orient="records")}


@app.get("/api/recursos_x_institucion")
async def contarRecursosInstitucion():
    data = pd.DataFrame(recursosTotales)
    total_x_institucion = data['nombre_institucion'].value_counts().reset_index(drop=False).rename(columns = {'count': "bases_publicadas"})
    tiene_plan_dict = {}
    for institucion in data['nombre_institucion']:
        tiene_plan_dict[institucion] = any(d['nombre_institucion'] == institucion and d['nombre_categoria'] == 'Plan de Apertura de Datos' for d in recursosTotales)
    total_x_institucion['tiene_plan'] = total_x_institucion['nombre_institucion'].apply(lambda x: tiene_plan_dict[x])
    total_x_institucion = total_x_institucion.reset_index(drop=False)
    return {"datum" : total_x_institucion.to_json(orient="records")}

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
    return {"df": reps.to_json(orient='records'), "promedio": promedio, "varianza": varianza, "desviacion": desviacion}


def formatearFecha(x, anio):
    anios = {"2026": 2026, 
             "26":2026, 
             "2025": 2025, 
             "25": 2025, 
             "2027": 2027, 
             "27": 2027
             }
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
             "diciembre": "12",
             "ene": "01", 
             "feb": "02", 
             "mar": "03", 
             "abr": "04", 
             "may": "05", 
             "jun": "06", 
             "jul":"07", 
             "ago": "08", 
             "sep": "09", 
             "oct": "10", 
             "nov": "11", 
             "dic": "12",
             "mzo": "03",
             "01": "01", 
             "02": "02", 
             "03": "03", 
             "04": "04", 
             "05": "05", 
             "06": "06", 
             "07":"07", 
             "08": "08", 
             "09": "09", 
             "10": "10", 
             "11": "11", 
             "12": "12"}
    
    palabra = x.replace("\r", '').replace("\n", '').replace("(", '').replace(")", "").strip().lower()
    try:
        # Intentamos parsearlo con la fecha con estructutra "anio-mes-dia"
        palabra = datetime.datetime.strptime(palabra, "%Y-%m-%d")
    except:
        try:
            # Intentamos parsearlo como fecha con estructura día-mes-anio 
            palabra = datetime.datetime.strptime(palabra, "%d-%m-%Y")
            palabra = datetime.strftime(palabra, '%Y-%m-%d')
        except:
            try:
                # Intentamos parsearlo como fecha con estructura día/mes/anio
                palabra = palabra.replace("/", "-")
                palabra = datetime.datetime.strptime(palabra, "%d-%m-%Y")
                palabra = datetime.strftime(palabra, '%Y-%m-%d')
            except:
                try: 
                    prueba = palabra.split(' ')
                    for i in range(len(prueba)):
                        prueba[i-1] = prueba[i-1].split('-')
                    prueba = list(itertools.chain(*prueba))
                    for i in range(len(prueba)):
                        prueba[i-1] = prueba[i-1].split('.')
                    prueba = list(itertools.chain(*prueba))
                    for i in range(len(prueba)):
                        prueba[i-1] = prueba[i-1].strip().replace("del", '').replace('de', '').replace(',', '')
                    prueba = list(filter(lambda x: x != '', prueba))
                    if len(prueba) == 1 and palabra in meses.keys():
                        palabra = f"{anio}-{meses[palabra]}-01"
                        palabra = datetime.datetime.strptime(palabra, "%Y-%m-%d")
                    elif len(prueba) == 3:
                        prueba[1] = meses[prueba[1]]
                        prueba = "-".join(prueba)
                        palabra = datetime.datetime.strptime(prueba, "%d-%m-%Y")
                        palabra = datetime.strftime(palabra, '%Y-%m-%d')
                    elif len(prueba) == 2:
                        if prueba[0] in meses.keys() and prueba[1] in anios.keys():
                            palabra = f"{anios[prueba[1]]}-{meses[prueba[0]]}-01"
                            palabra = datetime.datetime.strptime(palabra, "%Y-%m-%d")
                        elif prueba[0] in anios.keys() and len(prueba[0]) == 4 and prueba[1] in meses.keys():
                            palabra = f"{anios[prueba[0]]}-{meses[prueba[1]]}-01"
                            palabra = datetime.datetime.strptime(palabra, "%Y-%m-%d")
                        elif float(prueba[0]) and prueba[1] in meses.keys():
                            palabra = f"{anio}-{meses[prueba[1]]}-{prueba[0]}"
                            palabra = datetime.datetime.strptime(palabra, "%Y-%m-%d")
                        else:
                            #print("Estamos en el ultimo caso del array con 2 elementos: ", prueba)
                            return palabra
                    else:
                        #print("Estamos en el último caso del bloque if: ", prueba)
                        return palabra
                except:    
                    #print("No se pudo parsear como fecha", palabra)
                    return palabra
    return palabra


async def fetchCsv(session, url):
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
        "Descripción del conjunto de datos": "descripcion_conjunto_datos",
        "Periodicidad de publicación": "periodicidad_publicacion",
        "Área que genera el recurso de datos": "area_genera_recurso_datos",
        "A\u0081rea que genera el recurso de datos": "area_genera_recurso_datos"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-MX,es;q=0.9",
        "Referer": "https://www.datos.gob.mx/",
    }
    fecha = datetime.datetime.strptime(url['fecha'], "%Y-%m-%dT%H:%M:%S.%f")
    anio = fecha.year
    la_url = url['url']
    async with session.get(la_url, headers=headers) as request:
        if request.status == 200:
            raw = await request.read() 
            try:
                df = pd.read_csv(BytesIO(raw), encoding='utf-8', low_memory=False)
                df = df.rename(columns = dict_columnas)
                if df["fecha_publicacion"].dtypes == "str":
                    df['fecha_alternativa'] = df['fecha_publicacion'].apply(lambda x: formatearFecha(x, anio))
                    df['fecha_formateada'] = pd.to_datetime(df['fecha_alternativa'], format='%Y-%m-%d', errors='coerce').astype(str)
                return df
            except:
                try:
                    df = pd.read_csv(BytesIO(raw), encoding='latin-1', low_memory=False)
                    df = df.rename(columns = dict_columnas)
                    if df["fecha_publicacion"].dtypes == "str":
                        df['fecha_alternativa'] = df['fecha_publicacion'].apply(lambda x: formatearFecha(x, anio))
                        df['fecha_formateada'] = pd.to_datetime(df['fecha_alternativa'], format='%Y-%m-%d', errors='coerce').astype(str)
                    df =df.reset_index(drop=False)
                    return df
                except:
                    print("No se pudo obtener el df: ", url['recurso'])
                    return

        else:
            print("Fracasó la peticion del recurso: ", url['recurso'])
            return

@app.get("/api/planes_apertura")
async def obtenerPlanes(institucion):
    con_fechas = []
    no_fechas = []
    recursos = list(filter(lambda x: x["nombre_institucion"] == institucion, recursosTotales))
    planes = list(filter(lambda x: x["nombre_categoria"] == 'Plan de Apertura de Datos', recursos))
    if len(planes) == 0:
        raise HTTPException(status_code=404, detail="Esta institucion no subió su plan de apertura")

    listaUrls = list(map(lambda x: {'recurso': x['nombre_recurso'],'url': x['url_recurso'], 'fecha': x['creacion_recurso']}, planes))
    async with aiohttp.ClientSession() as session:
        tasks = [fetchCsv(session, url) for url in listaUrls]
        resultados = await asyncio.gather(*tasks, return_exceptions=False)

    df_all = pd.concat(resultados, ignore_index=True)
    no_fechas = df_all[df_all['fecha_formateada'].isna()].to_json(orient='records')
    con_fechas = df_all[df_all['fecha_formateada'].notna()].to_json(orient='records')
    return {"fechas_parseadas": con_fechas, "fechas_sin_parsear": no_fechas}