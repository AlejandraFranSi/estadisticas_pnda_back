
from io import BytesIO
import datetime
from fastapi import FastAPI, HTTPException
import pandas as pd
#import requests
import math
import asyncio
import aiohttp
import itertools
#import time
import mysql.connector
from io import StringIO, BytesIO
from aiohttp_client_cache import CachedSession, SQLiteBackend

from fastapi.middleware.cors import CORSMiddleware
#from pydantic import BaseModel

app = FastAPI(
    title="Documentación Reporte PNDA",
    description="Esta es la documentación de las peticiones que se pueden hacer para obtener datos del CKAN y del PNDASI para alimentar el front",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # puerto por defecto de Vite
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
#_cache = {}
#TTL_SEGUNDOS = 300  # Tiempo pra la cache
recursosTotales = []
hoy = datetime.datetime.today()
este_anio = hoy.year
inicio_temporal = pd.to_datetime("2025-01-01", yearfirst=True)
anio_inicio = 2025
inicio_trimestres = ["01-01", "04-01", "07-01", "10-01"]
fin_trimestres = ["03-31", "06-30", "09-30", "12-31"]

categorias_interaccion_dict = {
    "actualizar": "publicación", 
    "otros": "otros", 
    "solicitud_datos": "publicación", 
    "publicar": "publicación", 
    "enlace": "otros", 
    "dudas": "atención", 
    "solicitar_reunion": "atención", 
    "datos_nuevos": "publicación", 
    "datos_historicos": "publicación", 
    "acuse": "atención"
}

def obtener_trimestres():
    trimestres = {}
    for anio in range(int(anio_inicio), int(este_anio + 1)):
        i = 1
        for n in range(0, 4):
            trimestres[f"{i}-{anio}"] = [str(anio) + "-" + inicio_trimestres[n], str(anio) + "-" + fin_trimestres[n]]
            if i == 4:
                i = 1
            else:
                i += 1
    return trimestres

def identificar_trimestre(valor):
    trimestres = obtener_trimestres()
    fecha = pd.to_datetime(valor,yearfirst=True)
    for trimestre in trimestres.keys():
        # Es necesario especificar la hora porque si no obtenemos filas en las que no se puede asignar el trimestre porque la hora no cae dentro del rango
        if fecha >= pd.to_datetime(f"{trimestres[trimestre][0]}T00:00:00.0000000000", yearfirst=True) and fecha <= pd.to_datetime(f"{trimestres[trimestre][1]}T23:59:59.9999999999", yearfirst=True):
            return trimestre
                
def iterar_recursos(conjuntos):
    recursos = []
    for conjunto in conjuntos:
        for recurso in conjunto["resources"]:
            datum = {
                "id_conjunto": conjunto["id"],
                "nombre_conjunto": conjunto["title"],
                "clave_conjunto": conjunto["name"],
                "notas_conjunto": conjunto["notes"],
                "nombre_categoria": conjunto["groups"][0]["display_name"],
                "clave_categoria": conjunto["groups"][0]["name"],
                "descripcion_categoria": conjunto["groups"][0]["description"],
                "nombre_institucion": conjunto["organization"]["title"],
                "descripcion_institucion": conjunto["organization"]["description"],
                "id_recurso": recurso["id"],
                "nombre_recurso": recurso["name"],
                "descripcion_recurso": recurso["description"],
                "etiquetas": list(map(lambda x: x["name"], (conjunto["tags"]))),
                "url_recurso": recurso["url"],
                "frecuencia_actualizacion": recurso["update_frequency"],
                "creacion_recurso": recurso["created"],
                "actualizacion_recurso": recurso["metadata_modified"],
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

# Esta función ejecuta una única petición teniendo en cuenta los parámetros enviados
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
            #return response.error
            return "Ocurrio un error"
        
# Esta función crea una lista de parámetros que servirán como 
# un tipo de paginación de los recursos al hacer solicitudes a la API del CKAN de Datos abiertos
async def multiFetch():
    maxExpectedPackages = 2500
    block_size = 100
    numRequests = math.ceil(maxExpectedPackages / block_size) 
    params = list()
    conjuntos = []
    for i in range(numRequests):
        if i == 0:
            param = {"limit" : block_size - 1 , "offset" : i * block_size}
        else:
            param = {"limit" : block_size -1 , "offset" : i * block_size}
        params.append(param)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, param) for param in params]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)

    if "Ocurrio un error" in resultados:
            return "Error"
    else: 
        for resultado in resultados:
            conjuntos += resultado

        return conjuntos


@app.get("/api/recursos", tags=["Publicación"])
async def solicitar_todos_recursos():
    """
    Permite obtener la lista completa de recursos que viven en el CKAN, 
    así como la información de los conjuntos, las categorías existentes y 
    las etiquetas empleadas.
    """
    conjuntos = await multiFetch()
    #if conjuntos.status_code == 404:
    #    raise HTTPException(status_code=404, detail="No se pudo obtener la completud de la información")
    #else:
    recursos = iterar_recursos(conjuntos)
    global recursosTotales
    recursosTotales = recursos
    categorias = obtener_categorias(conjuntos)
    etiquetas = obtener_etiquetas(conjuntos)
    return {"conjuntos": conjuntos, "recursos": recursos, "categorias": categorias, "etiquetas": etiquetas}

## Estas funciones se usaban en la vista vieja de Nosotros
"""
@app.get("/api/recursos_x_categoria")
async def contarRecursosPorCategoria():
    data = pd.DataFrame(recursosTotales)
    data['mes'] = pd.to_datetime(data['creacion_recurso']).dt.month.apply(str)
    data['anio'] = pd.to_datetime(data['creacion_recurso']).dt.year.apply(str)
    data['fecha'] = data['mes'].str.cat(data['anio'], sep="/")
    datum = data[['nombre_categoria', 'fecha']].groupby(['nombre_categoria', 'fecha']).size()
    datum = datum.reset_index().rename(columns = {0: 'reps_sum', 'nombre_categoria': "categoria"})
    valor_maximo = int(datum['reps_sum'].max())
    valor_minimo = int(datum['reps_sum'].min())
    return {"datum" : datum.to_json(orient="records"), "max": valor_maximo, "min": valor_minimo}

@app.get("/api/recursos_x_dia")
async def contarRecursosPorDia():
    data = pd.DataFrame(recursosTotales)
    data['dia'] = pd.to_datetime(data['creacion_recurso']).dt.day.apply(str)
    data['mes'] = pd.to_datetime(data['creacion_recurso']).dt.month.apply(str)
    data['anio'] = pd.to_datetime(data['creacion_recurso']).dt.year.apply(str)
    data['fecha_parseada'] = data['anio'].str.cat([data['mes'], data['dia']], sep="/")
    datum = data['fecha_parseada'].value_counts().reset_index()
    valor_maximo = int(datum['count'].max())
    valor_minimo = int(datum['count'].min())
    return {"datum" : datum.to_json(orient="records"), "max": valor_maximo, "min": valor_minimo}


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

@app.get("/api/correos_intitucionales")
def correos_institucionales():
    # Establecemos la conexión con el servidor
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=".Afasa3113asafA.",
    database="pandasi",
    )
    mycursor = mydb.cursor(dictionary = True)
    # Obtenemos la tabla que nos interesa
    mycursor.execute("SELECT * FROM pandasi.vinculacion;")
    vinculacion = mycursor.fetchall()
    data_vinculacion = pd.DataFrame(vinculacion)
    # Hacemos una mini limpieza de la base, quitamos columnas vacías y agregamos columnas de interés
    data_vinculacion = data_vinculacion[['id_interaccion', 'clave_institucion', 'tipo_evento',
        'fecha_evento', 'objetivo_interaccion', 'estatus_atencion', 'estatus_respuesta']]
    data_vinculacion = data_vinculacion.dropna()
    data_vinculacion['mes'] = pd.to_datetime(data_vinculacion['fecha_evento']).dt.month.apply(str)
    data_vinculacion['anio'] = pd.to_datetime(data_vinculacion['fecha_evento']).dt.year.apply(str)
    data_vinculacion["mes_anio"] = data_vinculacion["fecha_evento"].apply(lambda x: str(pd.to_datetime(x).month) + "/" + str(pd.to_datetime(x).year))
    data_vinculacion["objetivo_agrupado"] = data_vinculacion["objetivo_interaccion"].apply(lambda x: categorias_interaccion_dict[x])
    # Agrupamos la información que nos interesa
    frec_objetivo_contacto = data_vinculacion[["mes_anio", "mes", "anio", "objetivo_agrupado"]].groupby(["mes_anio", "anio", "mes", "objetivo_agrupado"]).size().reset_index()
    frec_objetivo_contacto = frec_objetivo_contacto.sort_values(by = ["anio", "mes"])
    frec_objetivo_contacto = frec_objetivo_contacto.drop(columns = ["anio", "mes"])
    frec_objetivo_contacto = frec_objetivo_contacto.rename(columns = {0: "interacciones", "objetivo_agrupado": "objetivo_interaccion"})
    return {"data_agrupada" : frec_objetivo_contacto.to_json(orient="records")}
"""
@app.get("/api/bases_trimestrales", tags=["Publicación"])
async def obtener_bases_trimestrales():
    """
    Se hace la solicitud de la totalidad de los recursos que viven en el CKAN y 
    a cada recurso se le asocia el trimestre correspondiente. Luego se hace un conteo de los
    recursos subidos según el trimestre y ese dataframe se transforma en un json y se
    regresa como respuesta de la petición.
    """
    trimestres = obtener_trimestres()
    conjuntos = await multiFetch()
    recursos = iterar_recursos(conjuntos)
    recursos_df = pd.DataFrame(recursos)
    recursos_df = recursos_df.dropna()
    recursos_df["trimestre"] = pd.to_datetime(recursos_df["creacion_recurso"], yearfirst=True).apply(lambda x: identificar_trimestre(x))
    recursos_trimestrales = recursos_df["trimestre"].value_counts().reset_index()
    recursos_trimestrales["inicio_trimestre"] = recursos_trimestrales["trimestre"].apply(lambda x: pd.to_datetime(f"{trimestres[x][0]}T00:00:00.0000000000", yearfirst=True))
    recursos_trimestrales["fin_trimestre"] = recursos_trimestrales["trimestre"].apply(lambda x: pd.to_datetime(f"{trimestres[x][1]}T23:59:59.9999999999", yearfirst=True))
    recursos_trimestrales = recursos_trimestrales.rename(columns = {"count": "recursos"})
    recursos_trimestrales = recursos_trimestrales.sort_values(by = "inicio_trimestre")
    recursos_trimestrales = recursos_trimestrales.to_json(orient="records")
    return {"recursos":recursos_trimestrales}

@app.get("/api/categorias_bases_trimestrales", tags=["Publicación"])
async def categorias_bases_trimestrales(inicio, fin):
    """
    Se hace la solicitud de la totalidad de los recursos que viven en el CKAN y 
    luego se filtran según el intervalo temporal seleccionado; inicio y fin son cadenas que vienen de fechas con el formato "%Y-%m-%d".
    Luego se obtiene el total de recursos subidos por categoría en ese intervalo temporal,
    se obtienen las 3 categorías con mas recursos y las demás se agupan bajo la categoría "Otras".
    Posteriormente se hace el conteo de recursos según la categoría agrupada y ese dataframe se transforma en un json y se
    regresa como respuesta de la petición.
    """
    fecha_inicio = pd.to_datetime(inicio, yearfirst=True).date()
    fecha_fin = pd.to_datetime(fin, yearfirst=True).date()
    conjuntos = await multiFetch()
    recursos = iterar_recursos(conjuntos)
    recursos_df = pd.DataFrame(recursos)
    recursos_df = recursos_df.dropna()
    # Nos aseguramos de comparar date con date
    recursos_df["fecha"] = pd.to_datetime(recursos_df["creacion_recurso"], yearfirst=True).dt.date
    data_intervalo = recursos_df[recursos_df["fecha"] >= fecha_inicio]
    data_intervalo = data_intervalo[data_intervalo["fecha"] <= fecha_fin]
    # Obtenemos el número de recursos por categoría
    recursos_x_cat = data_intervalo["nombre_categoria"].value_counts().reset_index().sort_values(by = "count", ascending = False)
    recursos_x_cat = recursos_x_cat.rename(columns = {"count": "recursos"})
    lista_categorias = list(recursos_x_cat["nombre_categoria"])
    top_categorias = lista_categorias[0:3]
    otras_categorias = lista_categorias[3:len(lista_categorias)]
    # Mapeamos las categorías agrupadas
    data_intervalo["categoria_trimestral"] = data_intervalo["nombre_categoria"].apply(lambda x: x if x in top_categorias else "Otras")
    temporal_top_categoria = data_intervalo[["fecha", "categoria_trimestral"]].groupby(["fecha", "categoria_trimestral"]).size().unstack()
    temporal_top_categoria.columns.name = None
    temporal_top_categoria = temporal_top_categoria.fillna(0)
    datum = temporal_top_categoria.reset_index()
    datum["fecha"] = datum["fecha"].astype(str)
    datum = datum.to_json(orient="records")
    # Ahora calculamos el promedio, la varianza y la desviación
    temporal_top_categoria["total"] = temporal_top_categoria.sum(axis = 1)
    promedio_diario_trimestral = temporal_top_categoria["total"].mean()
    varianza_diaria_trimestral = temporal_top_categoria["total"].var()
    desviacion_diaria_trimestral = temporal_top_categoria["total"].std()
    maximo_diario_trimestral = temporal_top_categoria["total"].max()
    return {"top_categorias": top_categorias, 
            "otras_categorias": otras_categorias, 
            "promedio": promedio_diario_trimestral, 
            "desviacion": desviacion_diaria_trimestral,
            "varianza": varianza_diaria_trimestral,
            "maximo": maximo_diario_trimestral,
            "data": datum}

@app.get("/api/correos_trimestrales", tags=["Vinculación"])
def correos_trimestrales():
    """
    Se establece una conexión con un servidor de MySQL con la data del PNDASI.
    Se obtiene la tabla de vinculación que tiene como entradas los correos recibidos. 
    Luego se hace un conteo de los recursos subidos según el trimestre y ese 
    dataframe se transforma en un json y se regresa como respuesta de la petición.
    """
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=".Afasa3113asafA.",
    database="pandasi",
    )
    mycursor = mydb.cursor(dictionary = True)
    mycursor.execute("SELECT * FROM pandasi.vinculacion;")
    vinculacion = mycursor.fetchall()
    data_vinculacion = pd.DataFrame(vinculacion)
    trimestres = obtener_trimestres()
    # Hacemos una mini limpieza de la base, quitamos columnas vacías y agregamos columnas de interés
    data_vinculacion = data_vinculacion[['id_interaccion', 'clave_institucion', 'tipo_evento',
        'fecha_evento', 'objetivo_interaccion', 'estatus_atencion', 'estatus_respuesta']]
    datum = data_vinculacion.dropna()
    datum["trimestre"] = datum["fecha_evento"].apply(lambda x: identificar_trimestre(x))
    correos_trimestrales = datum["trimestre"].value_counts().reset_index()
    correos_trimestrales["inicio_trimestre"] = correos_trimestrales["trimestre"].apply(lambda x: pd.to_datetime(f"{trimestres[x][0]}T00:00:00.0000000000", yearfirst=True))
    correos_trimestrales["fin_trimestre"] = correos_trimestrales["trimestre"].apply(lambda x: pd.to_datetime(f"{trimestres[x][1]}T23:59:59.9999999999", yearfirst=True))
    correos_trimestrales = correos_trimestrales.rename(columns = {"count": "interacciones"})
    correos_trimestrales = correos_trimestrales.sort_values(by = "inicio_trimestre")
    correos_trimestrales = correos_trimestrales.to_json(orient="records")
    return {"interacciones":correos_trimestrales}


@app.get("/api/motivo_interacciones_trimestrales", tags=["Vinculación"])
async def motivo_interacciones_trimestrales(inicio, fin):
    """
    Se establece una conexión con un servidor de MySQL con la data del PNDASI.
    Se obtiene la tabla de vinculación que tiene como entradas los correos recibidos. La data de la tabla
    se filtra según el intervalo temporal seleccionado; inicio y fin son cadenas que vienen de fechas con el formato "%Y-%m-%d".
    Luego se hace una nueva columna en la cual se agrupan algunos motivos de los correos y se hace 
    el conteo de recursos según el motivo agrupado. Ese dataframe se transforma en un json y se
    regresa como respuesta de la petición.
    """
    fecha_inicial = pd.to_datetime(inicio, yearfirst=True).date()
    fecha_final = pd.to_datetime(fin, yearfirst=True).date()
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=".Afasa3113asafA.",
    database="pandasi",
    )
    mycursor = mydb.cursor(dictionary = True)
    mycursor.execute("SELECT * FROM pandasi.vinculacion;")
    vinculacion = mycursor.fetchall()
    data_vinculacion = pd.DataFrame(vinculacion)
    data_vinculacion["fecha_evento"] = pd.to_datetime(data_vinculacion["fecha_evento"], yearfirst=True).dt.date
    data_intervalo = data_vinculacion[data_vinculacion["fecha_evento"] >= fecha_inicial]
    data_intervalo = data_intervalo[data_vinculacion["fecha_evento"] <= fecha_final]
    data_intervalo["objetivo_interaccion_agrupado"] = data_intervalo["objetivo_interaccion"].apply(lambda x: categorias_interaccion_dict[x])
    lista_tipo_interacciones = ["atención", "otros", "publicación"]
    objetivo_interacciones_trimestrales = data_intervalo[["fecha_evento", "objetivo_interaccion_agrupado"]].groupby(["fecha_evento", "objetivo_interaccion_agrupado"]).size()
    objetivo_interacciones_trimestrales = objetivo_interacciones_trimestrales.unstack()
    objetivo_interacciones_trimestrales = objetivo_interacciones_trimestrales.fillna(0)
    objetivo_interacciones_trimestrales.columns.name = None
    datum = objetivo_interacciones_trimestrales.reset_index()
    datum["fecha_evento"] = datum["fecha_evento"].astype(str)
    datum = datum.to_json(orient="records")
    # Ahora calculamos el promedio, la varianza y la desviación
    objetivo_interacciones_trimestrales["total"] = objetivo_interacciones_trimestrales.sum(axis = 1)
    promedio_diario_trimestral = objetivo_interacciones_trimestrales["total"].mean()
    varianza_diaria_trimestral = objetivo_interacciones_trimestrales["total"].var()
    desviacion_diaria_trimestral = objetivo_interacciones_trimestrales["total"].std()
    maximo_diario_trimestral = objetivo_interacciones_trimestrales["total"].max()
    return {"tipo_interacciones": lista_tipo_interacciones,
            "promedio": promedio_diario_trimestral, 
            "desviacion": desviacion_diaria_trimestral,
            "varianza": varianza_diaria_trimestral,
            "maximo": maximo_diario_trimestral,
            "data": datum}

@app.get("/api/recursos_x_interaccion", tags=["Otros"])
async def recursos_x_interaccion(inicio, fin):
    fecha_inicio = pd.to_datetime(inicio, yearfirst=True).date()
    fecha_fin = pd.to_datetime(fin, yearfirst=True).date()
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password=".Afasa3113asafA.",
        database="pandasi",
        )
    mycursor = mydb.cursor(dictionary = True)
    # 1. Traemos la información de las instituciones del pndasi
    mycursor.execute("SELECT * FROM pandasi.instituciones;")
    instituciones = mycursor.fetchall()
    instituciones_pndasi = pd.DataFrame(instituciones)
    # 2. Formateamos el nombre de la institución
    instituciones_pndasi['nombre_formateado'] = instituciones_pndasi["nombre"].apply(lambda x: x.lower().rstrip('.'))
    # 3. Generamos un diccionario con claves y nombres de las intituciones del pndasi
    dict_instituciones_pndasi = instituciones_pndasi[["clave_institucion", "nombre_formateado"]].groupby('clave_institucion').sum().to_dict()["nombre_formateado"]
    # 4. Llamamos los datos de interacciones
    mycursor.execute("SELECT * FROM pandasi.vinculacion;")
    vinculacion = mycursor.fetchall()
    data_vinculacion = pd.DataFrame(vinculacion)
    # 5. Filtramos la data interacciones según la fecha
    data_vinculacion["fecha"] = pd.to_datetime(data_vinculacion["fecha_evento"]).dt.date
    data_pndasi = data_vinculacion[data_vinculacion["fecha"] >= fecha_inicio]
    data_pndasi = data_pndasi[data_pndasi["fecha"] <= fecha_fin]
    # 6. Contamos el número de correos por institución y mapeamos su nombre según su clave
    interacciones_x_institucion = data_pndasi['clave_institucion'].value_counts().reset_index()
    interacciones_x_institucion = interacciones_x_institucion.rename(columns = {'count': 'num_interacciones'})
    interacciones_x_institucion['nombre_institucion'] = interacciones_x_institucion['clave_institucion'].apply(lambda x: dict_instituciones_pndasi[x])
    # 7. Construimos un diccionario de interacciones por institución
    dict_interacciones_x_institucion = interacciones_x_institucion[["nombre_institucion", "num_interacciones"]].groupby('nombre_institucion').sum().to_dict()["num_interacciones"]
    # 8. Filtramos los datos del ckan segun el periodo temporal
    conjuntos = await multiFetch()
    recursos = iterar_recursos(conjuntos)
    recursos_df = pd.DataFrame(recursos)
    recursos_df["fecha"] = pd.to_datetime(recursos_df["creacion_recurso"]).dt.date
    data_ckan = recursos_df[pd.to_datetime(recursos_df["fecha"]) >= pd.to_datetime(inicio)]
    data_ckan = data_ckan[pd.to_datetime(data_ckan["fecha"]) <= pd.to_datetime(fin)]
    # 9. Contamos en número de recursos por institución en el ckan
    recursos_x_institucion = data_ckan.value_counts('nombre_institucion').reset_index()
    recursos_x_institucion = recursos_x_institucion.rename(columns = {'count': 'num_recursos'})
    # 10. Agregamos una columna con el nombre formateado
    recursos_x_institucion['nombre_formateado'] = recursos_x_institucion["nombre_institucion"].apply(lambda x: x.split(" (")[0].lower())
    # 11. Mapeamos el número de interacciones por institución
    recursos_x_institucion['num_interacciones']= recursos_x_institucion['nombre_formateado'].apply(lambda x: dict_interacciones_x_institucion[x] if x in dict_interacciones_x_institucion.keys() else 0)
    # 12. Generamos una lista de elementos únicos para saber las categorías asociadas a cada institución
    data_ckan['categoria_con_coma'] = data_ckan['nombre_categoria'] + ", "
    cuantas_categorias = data_ckan[["nombre_institucion", "categoria_con_coma"]].groupby("nombre_institucion").sum()
    cuantas_categorias['total_categorias'] = cuantas_categorias['categoria_con_coma']#.astype(str).apply(lambda x: list(set(x.strip().split(', '))))
    # 13. Generamos un diccionario con esa información
    categorias_x_institucion = cuantas_categorias["total_categorias"].to_dict()
    # 14. Mapeamos las categorías en la data de recursos por institución
    recursos_x_institucion['categorias'] = recursos_x_institucion["nombre_institucion"].apply(lambda x: categorias_x_institucion[x])
    recursos_x_institucion = recursos_x_institucion.sort_values(by = 'num_recursos', ascending = False)
    data = recursos_x_institucion[["nombre_institucion", "categorias", "num_recursos", "num_interacciones"]].to_json(orient="records")
    return {"result": data }

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

@app.get("/api/planes_apertura", tags=["Otros"])
async def obtener_planes_apertura(institucion):
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

cache_si = SQLiteBackend(expire_after = datetime.timedelta(days=30))

async def obtener_filas_en_plan(session, url):
    dict_columnas = {
        'Descripción del recurso de datos': 'descripcion_recurso_datos', 
        'Fecha publicacación 2026': 'fecha_publicacion', 
        'importancia_recurso': 'importancia_recurso', 
        'Fecha publicacion 2026': 'fecha_publicacion', 
        'Importancia del recurso': 'importancia_recurso', 
        'Periodicidad de publicación': 'periodicidad_publicacion', 
        'Descripcion del recurso de datos': 'descripcion_recurso_datos', 
        'Á\x81rea que genera el recurso de datos': 'area_genera_recurso_datos', 
        'A\x81rea que genera el recurso de datos': 'area_genera_recurso_datos', 
        'Descripción del conjunto de datos': 'descripcion_conjunto_datos', 
        'Fecha de publicacion 2026': 'fecha_publicacion', 
        'poblacion_objetivo_o_sector_uso': "poblacion_objetivo_sector_uso", 
        'Área que genera el recurso de datos': 'area_genera_recurso_datos', 
        'descripcion_recurso_datos': 'descripcion_recurso_datos', 
        'Fecha publicación 2026': 'fecha_publicacion', 
        'Periodicidad de\n publicación': 'periodicidad_publicacion', 
        'Poblacion objetivo o sector de uso':"poblacion_objetivo_sector_uso", 
        'area_que_genera_recurso_datos': 'area_genera_recurso_datos', 
        'Observaciones': 'observaciones', 
        'fecha_publicacion_2026': 'fecha_publicacion', 
        'periodicidad_publicacion': 'periodicidad_publicacion', 
        'Periodicidad de publicacion': 'periodicidad_publicacion', 
        'Población objetivo o sector de uso': "poblacion_objetivo_sector_uso", 
        'Recurso de datos': 'recurso_datos', 
        'ciudadanía objetivo o sector de uso': "poblacion_objetivo_sector_uso", 
        'Fecha de publicación 2026': 'fecha_publicacion', 
        'Fecha publicación\n 2026': 'fecha_publicacion', 
        'recurso_datos': 'recurso_datos', 
        'Ãrea que genera el recurso de datos': 'area_genera_recurso_datos', 
        'Descripción del conjunto de datos ': 'descripcion_conjunto_datos', 
        'conjunto_datos': 'conjunto_datos', 
        'Periodicidad de publicacioln': 'periodicidad_publicacion', 
        'Descripción n del recurso de datos': 'descripcion_recurso_datos', 
        'Area que genera el recurso de datos': 'area_genera_recurso_datos', 
        'Conjunto de Datos': 'conjunto_datos', 
        'Ã\x81rea que genera el recurso de datos': 'area_genera_recurso_datos', 
        'Formato del recurso': 'formato_recurso', 
        'Conjunto de datos': 'conjunto_datos', 
        'descripcion_conjunto_datos': 'descripcion_conjunto_datos', 
        'Descripcion del conjunto de datos': 'descripcion_conjunto_datos', 
        'Ã\uf181rea que genera el recurso de datos': 'area_genera_recurso_datos',
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
    anio = 2026
    la_url = url['url']
    async with session.get(la_url, headers=headers) as request:
        if request.status == 200:
            try:
                raw = await request.text()
                jsonfied = StringIO(raw)
                data = pd.read_csv(jsonfied, encoding='utf-8', low_memory=False)
                dataFrame = pd.DataFrame(data)
                dataFrame = dataFrame.dropna()
                dataFrame = dataFrame.rename(columns = dict_columnas)
                dataFrame['conjunto_datos_formateado'] = dataFrame['conjunto_datos'].apply(lambda x: x.strip().lower())
                conjuntos = dataFrame['conjunto_datos_formateado'].value_counts().reset_index()
                return {"institucion": url["institucion"], "conjuntos": conjuntos.shape[0]}
            except:
                retry_request = await session.get(la_url, headers=headers)
                if retry_request.status == 200:
                    retry_response = await retry_request.read()
                    retry_data = pd.read_csv(BytesIO(retry_response), encoding='utf-8', low_memory=False)
                    retry_dataFrame = pd.DataFrame(retry_data)
                    retry_dataFrame = retry_dataFrame.dropna()
                    retry_dataFrame = retry_dataFrame.rename(columns = dict_columnas)
                    retry_dataFrame['conjunto_datos_formateado'] = retry_dataFrame['conjunto_datos'].apply(lambda x: x.strip().lower())
                    conjuntos = retry_dataFrame['conjunto_datos_formateado'].value_counts().reset_index()
                    return {"institucion": url["institucion"], "conjuntos": conjuntos.shape[0]}
                else:
                    print("Fracasó el retry de: ", url['institucion'])
                    return f"{url["institucion"]}"
        else:
            return f"{url["institucion"]}"

async def obtener_data_ptar_planes(listaInst):
    listaUrls = list(map(lambda x: {'institucion': x['nombre_institucion'],'url': x['url_recurso']}, listaInst))
    instituciones_error = []
    conjuntos_x_institucion = {}
    async with CachedSession(cache=cache_si) as session:
        tasks = [obtener_filas_en_plan(session, url) for url in listaUrls]
        resultados = await asyncio.gather(*tasks, return_exceptions=False)
    for resultado in resultados:
        if type(resultado) == str:
            instituciones_error.append(resultado)
        else:
            conjuntos_x_institucion[resultado["institucion"]] = resultado["conjuntos"]
    return { "con_error": instituciones_error, "ok": conjuntos_x_institucion}

@app.get("/api/data_ptar", tags=["PTAR"])
async def obtener_informacion_ptar(anio_param):
    """
    Esta función obtiene el total de conjuntos publicados, actualizados y comprometidos por institución dado un año.
    Para obtener la información de los conjuntos comprometidos se hace una consulta al Plan de Apertura de cada institución y 
    se calcula el número de conjuntos encontrados. Posteriormente, usando la información del Sistema de Información se mapea el sector
    al que pertenece cada institución y se construye un dataframe que indica el número de instituciones por sector que publicaron en ese año.
    """
    anio = int(anio_param)
    # Conseguimos los recursos
    conjuntos = await multiFetch()
    recursos_lista = iterar_recursos(conjuntos)
    recursos = pd.DataFrame(recursos_lista)
    recursos = recursos.dropna()
    # Agregamos columnas de añio de carga y de actualización
    recursos["anio_carga"] = pd.to_datetime(recursos["creacion_recurso"]).dt.year
    recursos["anio_actualizacion"] = pd.to_datetime(recursos["actualizacion_recurso"]).dt.year
    # Nos quedamos con los planes de apertura del año seleccionado
    planes_apertura = recursos[recursos["clave_categoria"] == 'plan_apertura_datos']
    planes_apertura = planes_apertura[planes_apertura['anio_carga'] == anio]
    # Ahora nos quedamos con los recursos reales: tanto los subidos como los actualizados
    bases_datos = recursos[recursos["clave_categoria"] != 'plan_apertura_datos']
    recursos_subidos = bases_datos[bases_datos["anio_carga"] == anio]
    recursos_actualizados = bases_datos[bases_datos["anio_actualizacion"] == anio]
    recursos_actualizados = recursos_actualizados[recursos_actualizados['anio_carga'] != anio]
    # Contamos el total de conjuntos subidos por institución
    conjuntos_subidos_xinst = recursos_subidos[["nombre_institucion", "nombre_conjunto"]].groupby(["nombre_institucion", "nombre_conjunto"]).sum().reset_index()
    conjuntos_subidos_xinst = conjuntos_subidos_xinst["nombre_institucion"].value_counts().reset_index().groupby("nombre_institucion").sum()
    conjuntos_subidos_xinst = conjuntos_subidos_xinst.rename(columns = {"count": "creados"})
    # Ahora contamos los conjuntos actualizados por institución
    conjuntos_actualizados_xinst = recursos_actualizados[["nombre_institucion", "nombre_conjunto"]].groupby(["nombre_institucion", "nombre_conjunto"]).sum().reset_index()
    conjuntos_actualizados_xinst = conjuntos_actualizados_xinst["nombre_institucion"].value_counts().reset_index().groupby("nombre_institucion").sum()
    conjuntos_actualizados_xinst = conjuntos_actualizados_xinst.rename(columns = {"count": "actualizados"})
    # Concatenamos los recursos subidos con los actualizados
    conjuntos_x_institucion = pd.concat([conjuntos_actualizados_xinst, conjuntos_subidos_xinst]).fillna(0)
    # Le agregamos una columna con el total
    conjuntos_x_institucion["conjuntos_totales"] = conjuntos_x_institucion.sum(axis = 1)
    conjuntos_x_institucion = conjuntos_x_institucion.reset_index()
    # Ahora obtenemos la lista de urls por institución
    plan_institucional_url = planes_apertura[["nombre_institucion", "url_recurso"]].to_dict(orient='records')
    # Y solicitamos el número de conjuntos que traen sus planes
    #try:
    #    info_planes_api = await obtener_data_ptar_planes(plan_institucional_url)
    #    instituciones_con_error = info_planes_api["con_error"]
    #    conjunto_x_institucion_dict = info_planes_api["ok"]
    #except:
    instituciones_con_error = []
    conjunto_x_institucion_dict = pd.read_csv("filas_en_planes_x_institucion.csv")
    conjunto_x_institucion_dict = conjunto_x_institucion_dict.drop(columns = "Unnamed: 0")
    conjunto_x_institucion_dict = conjunto_x_institucion_dict.groupby("institucion").sum().to_dict()["filas_en_plan"]
    # Ahora mapeamos el número de conjuntos que cada institución indica que va a publicar
    conjuntos_x_institucion["conjunto_plan"] = conjuntos_x_institucion["nombre_institucion"].apply(lambda x: "Error" if x in instituciones_con_error 
                                           else conjunto_x_institucion_dict[x] if x in conjunto_x_institucion_dict.keys()
                                           else "Sin plan")
    # Ahora le vamos a agregar la informacion del sector, para eso necesitamos la data de las instituciones
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password=".Afasa3113asafA.",
        database="pandasi",
        )
    mycursor = mydb.cursor(dictionary = True)
    mycursor.execute("SELECT * FROM pandasi.instituciones;")
    vinculacion = mycursor.fetchall()
    instituciones_pndasi = pd.DataFrame(vinculacion)
    # Formateamos los nombres de las instituciones
    instituciones_pndasi['nombre_formateado'] = instituciones_pndasi["nombre"].apply(lambda x: x.lower().rstrip('.'))
    conjuntos_x_institucion["nombre_formateado"] = conjuntos_x_institucion['nombre_institucion'].apply(lambda x: x.split(" (")[0].rstrip(".").lower())
    # Obtenemos un diccionario donde las claves sean la institución y los valores el sector
    dict_sectores = instituciones_pndasi[["nombre_formateado", "sector"]].groupby(["nombre_formateado"]).sum().to_dict()["sector"]
    # Mapeamos el sector a la base de conjuntos por institución
    conjuntos_x_institucion["sector"] = conjuntos_x_institucion["nombre_formateado"].apply(lambda x: dict_sectores[x] if x in dict_sectores.keys() else "Sin sector asignado")
    # Obtenemos el agrupado de instituciones por sector
    instituciones_x_sector = conjuntos_x_institucion["sector"].value_counts().reset_index()
    instituciones_x_sector = instituciones_x_sector.rename(columns = {'count': 'instituciones_publicantes'})
    instituciones_x_sector = instituciones_x_sector.sort_values(by = 'sector')
    # Pasamos la data que necesitamos en front al formato json para que pueda ser leída
    conjuntos_x_institucion = conjuntos_x_institucion.to_json(orient='records')
    instituciones_x_sector = instituciones_x_sector.to_json(orient='records')
    return {"conjuntos_por_institucion": conjuntos_x_institucion, "instituciones_por_sector": instituciones_x_sector}
