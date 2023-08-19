from cvlac.db_cvlac import session as cvlac_session
from cvlac.db_gruplac import session as gruplac_session

import pandas as pd
import numpy as np
import json
import re
import shutil
import os

try:
    print("######### INICIO DE PREPROCESAMIENTO ############")
    shutil.rmtree('dashboard/assets/data/preprocessed_data')
    os.mkdir('dashboard/assets/data/preprocessed_data')
    print('preprocessed folder created')
except Exception as e:
    os.mkdir('dashboard/assets/data/preprocessed_data')
    print(e)
    pass

try:
    ############################################
    #SELECCIÓN DE TABLAS EN BASES DE DATOS: OPCIÓN 1
    ###########################################
    print("")
    print("RECUPERANDO TABLAS DE BASES DE DATOS...")
    #cvlac
    gruplac_articulos = pd.read_sql_query('SELECT * FROM articulos', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_articulos=gruplac_articulos.drop('id',axis=1)
    gruplac_basico = pd.read_sql_query('SELECT * FROM basico', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    #gruplac_basico=gruplac_basico.drop('id',axis=1)
    gruplac_caplibros = pd.read_sql_query('SELECT * FROM caplibros', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_caplibros=gruplac_caplibros.drop('id',axis=1)
    gruplac_integrantes = pd.read_sql_query('SELECT * FROM integrantes', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_integrantes=gruplac_integrantes.drop('id',axis=1)
    gruplac_libros = pd.read_sql_query('SELECT * FROM libros', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_libros=gruplac_libros.drop('id',axis=1)
    gruplac_oarticulos = pd.read_sql_query('SELECT * FROM otros_articulos', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_oarticulos=gruplac_oarticulos.drop('id',axis=1)
    gruplac_olibros = pd.read_sql_query('SELECT * FROM otros_libros', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_olibros=gruplac_olibros.drop('id',axis=1)
    gruplac_cdoctorado= pd.read_sql_query('SELECT * FROM curso_doctorado', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_cdoctorado=gruplac_cdoctorado.drop('id',axis=1)
    gruplac_cmaestria= pd.read_sql_query('SELECT * FROM curso_maestria', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_cmaestria=gruplac_cmaestria.drop('id',axis=1)
    gruplac_disenoind= pd.read_sql_query('SELECT * FROM diseno_industrial', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_disenoind=gruplac_disenoind.drop('id',axis=1)
    gruplac_empresatec= pd.read_sql_query('SELECT * FROM empresa_tecnologica', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_empresatec=gruplac_empresatec.drop('id',axis=1)
    gruplac_innovaempresa= pd.read_sql_query('SELECT * FROM innovacion_empresarial', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_innovaempresa=gruplac_innovaempresa.drop('id',axis=1)
    gruplac_instituciones= pd.read_sql_query('SELECT * FROM instituciones', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_instituciones=gruplac_instituciones.drop('id',axis=1)
    gruplac_lineas= pd.read_sql_query('SELECT * FROM lineas', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_lineas=gruplac_lineas.drop('id',axis=1)
    gruplac_otecnologicos= pd.read_sql_query('SELECT * FROM otros_tecnologicos', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_otecnologicos=gruplac_otecnologicos.drop('id',axis=1)
    gruplac_pdoctorado= pd.read_sql_query('SELECT * FROM programa_doctorado', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_pdoctorado=gruplac_pdoctorado.drop('id',axis=1)
    gruplac_plantapiloto= pd.read_sql_query('SELECT * FROM planta_piloto', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_plantapiloto=gruplac_plantapiloto.drop('id',axis=1)
    gruplac_pmaestria= pd.read_sql_query('SELECT * FROM programa_maestria', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_pmaestria=gruplac_pmaestria.drop('id',axis=1)
    gruplac_prototipos= pd.read_sql_query('SELECT * FROM prototipos', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_prototipos=gruplac_prototipos.drop('id',axis=1)
    gruplac_software= pd.read_sql_query('SELECT * FROM software', gruplac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    gruplac_software=gruplac_software.drop('id',axis=1)
        
    cvlac_areas=pd.read_sql_query('SELECT * FROM actuacion', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_areas=cvlac_areas.drop('id',axis=1)
    cvlac_articulos=pd.read_sql_query('SELECT * FROM articulos', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_articulos=cvlac_articulos.drop('id',axis=1)
    cvlac_basico=pd.read_sql_query('SELECT * FROM basico', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    #cvlac_basico=cvlac_basico.drop('id',axis=1)
    cvlac_lineas=pd.read_sql_query('SELECT * FROM investigacion', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_lineas=cvlac_lineas.drop('id',axis=1)
    cvlac_libros=pd.read_sql_query('SELECT * FROM libros', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_libros=cvlac_libros.drop('id',axis=1)
    cvlac_reconocimiento=pd.read_sql_query('SELECT * FROM reconocimiento', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_reconocimiento=cvlac_reconocimiento.drop('id',axis=1)
    cvlac_caplibros=pd.read_sql_query('SELECT * FROM caplibros', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_caplibros=cvlac_caplibros.drop('id',axis=1)
    cvlac_empresatec=pd.read_sql_query('SELECT * FROM empresa_tecnologica', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_empresatec=cvlac_empresatec.drop('id',axis=1)
    cvlac_innovaempresa=pd.read_sql_query('SELECT * FROM innovacion_empresarial', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_innovaempresa=cvlac_innovaempresa.drop('id',axis=1)
    cvlac_prototipos=pd.read_sql_query('SELECT * FROM prototipo', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_prototipos=cvlac_prototipos.drop('id',axis=1)
    cvlac_software=pd.read_sql_query('SELECT * FROM software', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_software=cvlac_software.drop('id',axis=1)
    cvlac_tecnologicos=pd.read_sql_query('SELECT * FROM tecnologicos', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_tecnologicos=cvlac_tecnologicos.drop('id',axis=1)
    cvlac_identificadores=pd.read_sql_query('SELECT * FROM identificadores', cvlac_session.bind, dtype = str).replace({'NaN':np.nan,'N.A':np.nan, '':np.nan, 'N/A':np.nan})
    cvlac_identificadores=cvlac_identificadores.drop('id',axis=1)
    gruplac_duplicados={'articulos':[],'libros':[],'oarticulos':[],'libros':[],'olibros':[],'caplibros':[],'otecnologicos':[],'software':[],'prototipos':[]}
except Exception as e:
    #raise
    print(e)
    ############################################
    #ENTRADA DE DATOS EXTRAIDOS: OPCION 2
    ############################################
    print("")
    print("RECUPERANDO TABLAS LOCALES EXTRAIDAS...")
    gruplac_articulos = pd.read_csv('dashboard/assets/data/extracted_data/aux_articulosg.csv', dtype = str)
    gruplac_basico = pd.read_csv('dashboard/assets/data/extracted_data/aux_basicog.csv', dtype = str)
    gruplac_caplibros = pd.read_csv('dashboard/assets/data/extracted_data/aux_caplibrosg.csv', dtype = str)
    gruplac_integrantes = pd.read_csv('dashboard/assets/data/extracted_data/aux_integrantes.csv', dtype = str)
    gruplac_libros = pd.read_csv('dashboard/assets/data/extracted_data/aux_librosg.csv', dtype = str) 
    gruplac_oarticulos = pd.read_csv('dashboard/assets/data/extracted_data/aux_oarticulos.csv', dtype = str)
    gruplac_olibros = pd.read_csv('dashboard/assets/data/extracted_data/aux_olibros.csv', dtype = str)
    gruplac_cdoctorado= pd.read_csv('dashboard/assets/data/extracted_data/aux_cdoctorado.csv', dtype = str)
    gruplac_cmaestria= pd.read_csv('dashboard/assets/data/extracted_data/aux_cmaestria.csv', dtype = str)
    gruplac_disenoind= pd.read_csv('dashboard/assets/data/extracted_data/aux_disenoind.csv', dtype = str)
    gruplac_empresatec= pd.read_csv('dashboard/assets/data/extracted_data/aux_empresatecg.csv', dtype = str)
    gruplac_innovaempresa= pd.read_csv('dashboard/assets/data/extracted_data/aux_innovaempresag.csv', dtype = str)
    gruplac_instituciones= pd.read_csv('dashboard/assets/data/extracted_data/aux_instituciones.csv', dtype = str)
    gruplac_lineas= pd.read_csv('dashboard/assets/data/extracted_data/aux_lineas.csv', dtype = str)
    gruplac_otecnologicos= pd.read_csv('dashboard/assets/data/extracted_data/aux_otecnologicos.csv', dtype = str)
    gruplac_pdoctorado= pd.read_csv('dashboard/assets/data/extracted_data/aux_pdoctorado.csv', dtype = str)
    gruplac_plantapiloto= pd.read_csv('dashboard/assets/data/extracted_data/aux_plantapilotog.csv', dtype = str)
    gruplac_pmaestria= pd.read_csv('dashboard/assets/data/extracted_data/aux_pmaestria.csv', dtype = str)
    gruplac_prototipos= pd.read_csv('dashboard/assets/data/extracted_data/aux_prototiposg.csv', dtype = str)
    gruplac_software= pd.read_csv('dashboard/assets/data/extracted_data/aux_softwareg.csv', dtype = str)
    
    cvlac_areas=pd.read_csv('dashboard/assets/data/extracted_data/aux_actuacion.csv', dtype = str)
    cvlac_articulos=pd.read_csv('dashboard/assets/data/extracted_data/aux_articulos.csv', dtype = str)
    cvlac_basico=pd.read_csv('dashboard/assets/data/extracted_data/aux_basico.csv', dtype = str)
    cvlac_lineas=pd.read_csv('dashboard/assets/data/extracted_data/aux_investigacion.csv', dtype = str)
    cvlac_libros=pd.read_csv('dashboard/assets/data/extracted_data/aux_libros.csv', dtype = str)
    cvlac_reconocimiento=pd.read_csv('dashboard/assets/data/extracted_data/aux_reconocimiento.csv', dtype = str)
    cvlac_caplibros=pd.read_csv('dashboard/assets/data/extracted_data/aux_caplibros.csv', dtype = str)
    cvlac_empresatec=pd.read_csv('dashboard/assets/data/extracted_data/aux_empresatec.csv', dtype = str)
    cvlac_innovaempresa=pd.read_csv('dashboard/assets/data/extracted_data/aux_innovaempresa.csv', dtype = str)
    cvlac_prototipos=pd.read_csv('dashboard/assets/data/extracted_data/aux_prototipo.csv', dtype = str)
    cvlac_software=pd.read_csv('dashboard/assets/data/extracted_data/aux_software.csv', dtype = str)
    cvlac_tecnologicos=pd.read_csv('dashboard/assets/data/extracted_data/aux_tecnologicos.csv', dtype = str)
    cvlac_identificadores=pd.read_csv('dashboard/assets/data/extracted_data/aux_identificadores.csv', dtype = str)
    gruplac_duplicados={'articulos':[],'libros':[],'oarticulos':[],'libros':[],'olibros':[],'caplibros':[],'otecnologicos':[],'software':[],'prototipos':[]}

###########################################
#PREPROCESSING
##########################################
print("Limpiando datos...")
#LIMPIEZA DE TABLAS CVLAC

cvlac_basico['categoria']=cvlac_basico['categoria'].fillna('No Aplica').astype(str).str.extract(r'(^[^(]*)',expand=False).replace('','No Aplica')
cvlac_basico['sexo']=cvlac_basico['sexo'].replace('','No Aplica').fillna('No Aplica')
cvlac_areas['areas']=cvlac_areas['areas'].astype(str).replace(' -- ',';',regex=True)
cvlac_articulos['tipo']=cvlac_articulos['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^-]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_articulos['sectores']=cvlac_articulos['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
cvlac_articulos['lugar']=cvlac_articulos['lugar'].fillna('No Aplica')
#remover duplicados por doi,idgruplac y mantener el registro con mas columnas rellenadas
cvlac_articulos['issn']=cvlac_articulos['issn'].replace('','No Aplica',regex=True).fillna('No Aplica')
cvlac_articulos['fecha']=pd.to_datetime(cvlac_articulos['fecha']).dt.to_period('Y')
cvlac_articulos['palabras']=cvlac_articulos['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_articulos['revista']=cvlac_articulos['revista'].fillna('No Aplica')
col=cvlac_articulos.copy()
col['revista']=col['revista'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
byrevi_normalized=col.groupby('revista')
for key in list(byrevi_normalized.groups.keys()):  
    cvlac_articulos.iloc[list(byrevi_normalized.get_group(key).index),6]=cvlac_articulos.iloc[list(byrevi_normalized.get_group(key).index)]['revista'].value_counts().index[0]
byrevi_normalized=0
col['editorial']=col['editorial'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('editorial')
col=0
for key in list(bydept_normalized.groups.keys()):  
    cvlac_articulos.iloc[list(bydept_normalized.get_group(key).index),8]=cvlac_articulos.iloc[list(bydept_normalized.get_group(key).index)]['editorial'].value_counts().index[0]
bydept_normalized=0
cvlac_lineas['nombre']=cvlac_lineas['nombre'].replace('','No Aplica')
cvlac_libros['tipo']=cvlac_libros['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^-]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_libros['lugar']=cvlac_libros['lugar'].fillna('No Aplica')
#remover  duplicados por (isbn limpio),idgruplac mantener el registro con mas columnas rellenas
cvlac_libros['fecha']=pd.to_datetime(cvlac_libros['fecha']).dt.to_period('Y')
cvlac_libros['editorial']=cvlac_libros['editorial'].fillna('No Aplica')
cvlac_libros['palabras']=cvlac_libros['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_libros['areas']=cvlac_libros['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_libros['sectores']=cvlac_libros['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
col=cvlac_libros.copy()
col['editorial']=col['editorial'].str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9]','', regex=True).str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip() 
byedit_normalized=col.groupby('editorial')
for key in list(byedit_normalized.groups.keys()):  
    cvlac_libros.iloc[list(byedit_normalized.get_group(key).index),7]=cvlac_libros.iloc[list(byedit_normalized.get_group(key).index)]['editorial'].value_counts().index[0]
byedit_normalized=0
col=0
cvlac_reconocimiento['fecha']=pd.to_datetime(cvlac_reconocimiento['fecha'].astype(str).str.extract(r'([^ ]*$)',expand=False).str.strip()).dt.to_period('Y')
cvlac_caplibros['libro']=cvlac_caplibros['libro'].replace('&gt;','',regex=True)
cvlac_caplibros['lugar']=cvlac_caplibros['lugar'].fillna('No Aplica')
#remover duplicados por capitulo,idgruplac,paginas y matener el de mas columnas rellenadas
cvlac_caplibros['fecha']=pd.to_datetime(cvlac_caplibros['fecha']).dt.to_period('Y')
cvlac_caplibros['editorial']=cvlac_caplibros['editorial'].fillna('No Aplica')
cvlac_caplibros['areas']=cvlac_caplibros['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_caplibros['palabras']=cvlac_caplibros['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_caplibros['sectores']=cvlac_caplibros['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
col=cvlac_caplibros.copy()
col['editorial']=col['editorial'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
byedit_normalized=col.groupby('editorial')
for key in list(byedit_normalized.groups.keys()):  
    cvlac_caplibros.iloc[list(byedit_normalized.get_group(key).index),6]=cvlac_caplibros.iloc[list(byedit_normalized.get_group(key).index)]['editorial'].value_counts().index[0]
byedit_normalized=0
col['capitulo']=col['capitulo'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
bydept_normalized=col.groupby('capitulo')
for key in list(bydept_normalized.groups.keys()):  
    cvlac_caplibros.iloc[list(bydept_normalized.get_group(key).index),13]=cvlac_caplibros.iloc[list(bydept_normalized.get_group(key).index)]['capitulo'].value_counts().index[0]
bydept_normalized=0
cvlac_caplibros['nombre']=cvlac_caplibros['capitulo'].copy()
col['libro']=col['libro'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('libro')
col=0
for key in list(bydept_normalized.groups.keys()):  
    cvlac_caplibros.iloc[list(bydept_normalized.get_group(key).index),14]=cvlac_caplibros.iloc[list(bydept_normalized.get_group(key).index)]['libro'].value_counts().index[0]
bydept_normalized=0
cvlac_empresatec['registro_camara']=pd.to_datetime(cvlac_empresatec['registro_camara']).dt.to_period('M')
cvlac_empresatec['tipo']=cvlac_empresatec['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^ - ]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_empresatec['palabras']=cvlac_empresatec['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_empresatec['areas']=cvlac_empresatec['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_empresatec['sectores']=cvlac_empresatec['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
cvlac_innovaempresa['tipo']=cvlac_innovaempresa['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^-]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_innovaempresa['lugar']=cvlac_innovaempresa['lugar'].fillna('No Aplica')
cvlac_innovaempresa['fecha']=pd.to_datetime(cvlac_innovaempresa['fecha']).dt.to_period('Y')
cvlac_innovaempresa['palabras']=cvlac_innovaempresa['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_innovaempresa['areas']=cvlac_innovaempresa['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_innovaempresa['sectores']=cvlac_innovaempresa['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
cvlac_prototipos['tipo']=cvlac_prototipos['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^-]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_prototipos['lugar']=cvlac_prototipos['lugar'].fillna('No Aplica')
cvlac_prototipos['fecha']=pd.to_datetime(cvlac_prototipos['fecha']).dt.to_period('Y')
cvlac_prototipos['palabras']=cvlac_prototipos['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_prototipos['areas']=cvlac_prototipos['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_prototipos['sectores']=cvlac_prototipos['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
cvlac_software['tipo']=cvlac_software['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^-]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_software['lugar']=cvlac_software['lugar'].fillna('No Aplica')
cvlac_software['fecha']=pd.to_datetime(cvlac_software['fecha']).dt.to_period('Y')
cvlac_software['palabras']=cvlac_software['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_software['areas']=cvlac_software['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_software['sectores']=cvlac_software['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()
cvlac_tecnologicos['tipo']=cvlac_tecnologicos['tipo'].fillna('No Aplica').astype(str).str.extract(r'([^-]*$)',expand=False).replace('','No Aplica').str.strip()
cvlac_tecnologicos['lugar']=cvlac_tecnologicos['lugar'].fillna('No Aplica')
cvlac_tecnologicos['fecha']=pd.to_datetime(cvlac_tecnologicos['fecha']).dt.to_period('Y')
cvlac_tecnologicos['palabras']=cvlac_tecnologicos['palabras'].fillna('No Aplica').astype(str).replace(', ',';',regex=True)
cvlac_tecnologicos['areas']=cvlac_tecnologicos['areas'].fillna('No Aplica').astype(str).replace(' -- ',';',regex=True)
cvlac_tecnologicos['sectores']=cvlac_tecnologicos['sectores'].fillna('No Aplica').astype(str).str.extract(r'(^[^-]*)',expand=False).replace('','No Aplica').str.strip()

cvlac_articulos.to_csv('dashboard/assets/data/preprocessed_data/cvlac_articulos.csv',index=False)
cvlac_basico.to_csv('dashboard/assets/data/preprocessed_data/cvlac_basico.csv',index=False)
cvlac_caplibros.to_csv('dashboard/assets/data/preprocessed_data/cvlac_caplibros.csv',index=False)
cvlac_libros.to_csv('dashboard/assets/data/preprocessed_data/cvlac_libros.csv',index=False)
cvlac_empresatec.to_csv('dashboard/assets/data/preprocessed_data/cvlac_empresatec.csv',index=False)
cvlac_innovaempresa.to_csv('dashboard/assets/data/preprocessed_data/cvlac_innovaempresa.csv',index=False)
cvlac_lineas.to_csv('dashboard/assets/data/preprocessed_data/cvlac_lineas.csv',index=False)
cvlac_tecnologicos.to_csv('dashboard/assets/data/preprocessed_data/cvlac_otecnologicos.csv',index=False)
cvlac_prototipos.to_csv('dashboard/assets/data/preprocessed_data/cvlac_prototipos.csv',index=False)
cvlac_software.to_csv('dashboard/assets/data/preprocessed_data/cvlac_software.csv',index=False)
cvlac_areas.to_csv('dashboard/assets/data/preprocessed_data/cvlac_areas.csv',index=False)
cvlac_reconocimiento.to_csv('dashboard/assets/data/preprocessed_data/cvlac_reconocimiento.csv',index=False)
cvlac_identificadores.to_csv('dashboard/assets/data/preprocessed_data/cvlac_identificadores.csv',index=False)

#LIMPIEZA DE TABLAS GRUPLAC

gruplac_basico['fecha_formacion']=pd.to_datetime(gruplac_basico['fecha_formacion']).dt.to_period('M')#DTYPE: period[M]
gruplac_basico['clasificacion']=gruplac_basico['clasificacion'].fillna('No Aplica').astype(str).str.extract(r'([^\s]+)',expand=False).replace('','No Aplica')
gruplac_basico['areas']=gruplac_basico['areas'].astype(str).replace(' -- ',';',regex=True)
gruplac_basico['nombre']=gruplac_basico['nombre'].str.strip()
col=gruplac_basico.copy()
col['programas']=col['programas'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('programas')
for key in list(bydept_normalized.groups.keys()):  
    gruplac_basico.iloc[list(bydept_normalized.get_group(key).index),10]=gruplac_basico.iloc[list(bydept_normalized.get_group(key).index)]['programas'].value_counts().index[0]
bydept_normalized=0
col['programas_secundario']=col['programas_secundario'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('programas_secundario')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_basico.iloc[list(bydept_normalized.get_group(key).index),11]=gruplac_basico.iloc[list(bydept_normalized.get_group(key).index)]['programas_secundario'].value_counts().index[0]
bydept_normalized=0
#CREAR VALORES UNICOS PARA FILTROS
#set_areas=set()
#gruplac_basico['areas'].apply(lambda x: set_areas.update(x.split(';')))
col=gruplac_instituciones.copy()
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_instituciones.iloc[list(bydept_normalized.get_group(key).index),1]=gruplac_instituciones.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_articulos['lugar']=gruplac_articulos['lugar'].fillna('No Aplica')
#remover duplicados por doi,idgruplac y mantener el registro con mas columnas rellenadas
gruplac_articulos['issn']=gruplac_articulos['issn'].replace('^0$','No Aplica',regex=True).fillna('No Aplica')
gruplac_articulos['fecha']=pd.to_datetime(gruplac_articulos['fecha']).dt.to_period('Y')
gruplac_articulos['revista']=gruplac_articulos['revista'].fillna('No Aplica')
col=gruplac_articulos.copy()
col['revista']=col['revista'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
byrevi_normalized=col.groupby('revista')
for key in list(byrevi_normalized.groups.keys()):  
    gruplac_articulos.iloc[list(byrevi_normalized.get_group(key).index),5]=gruplac_articulos.iloc[list(byrevi_normalized.get_group(key).index)]['revista'].value_counts().index[0]
byrevi_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_articulos.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_articulos.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_caplibros['lugar']=gruplac_caplibros['lugar'].fillna('No Aplica')
#remover duplicados por capitulo,idgruplac,paginas y matener el de mas columnas rellenadas
gruplac_caplibros['fecha']=pd.to_datetime(gruplac_caplibros['fecha']).dt.to_period('Y')
gruplac_caplibros['editorial']=gruplac_caplibros['editorial'].fillna('No Aplica')
col=gruplac_caplibros.copy()
col['editorial']=col['editorial'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
byedit_normalized=col.groupby('editorial')
for key in list(byedit_normalized.groups.keys()):  
    gruplac_caplibros.iloc[list(byedit_normalized.get_group(key).index),10]=gruplac_caplibros.iloc[list(byedit_normalized.get_group(key).index)]['editorial'].value_counts().index[0]
byedit_normalized=0
col['capitulo']=col['capitulo'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
bydept_normalized=col.groupby('capitulo')
for key in list(bydept_normalized.groups.keys()):  
    gruplac_caplibros.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_caplibros.iloc[list(bydept_normalized.get_group(key).index)]['capitulo'].value_counts().index[0]
bydept_normalized=0
col['libro']=col['libro'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('libro')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_caplibros.iloc[list(bydept_normalized.get_group(key).index),6]=gruplac_caplibros.iloc[list(bydept_normalized.get_group(key).index)]['libro'].value_counts().index[0]
bydept_normalized=0
pattern='|'.join(['Universidad del Cauca','Editorial Universidad Del Cauca','Ediorial Universidad Del Cauca','Sello Editorial Universidad del Cauca','Taller Editorial Universidad Del Cauca','Editorial De La Universidad Del Cauca','Univesidad Del Cauca','Talleres de impresión de la Universidad del Cauca','Talleres de Impresión la Universidad el Cauca','Centro De Publicaciones Universidad Del Cauca'])
gruplac_caplibros['editorial']=gruplac_caplibros['editorial'].str.replace(pattern, 'Editorial Universidad del Cauca',regex=True)
pattern='|'.join(['Sello editorial Uniautónoma del Cauca','Editorial de la Corporación Universitaria Autónoma del Cauca','Uniautónoma del Cauca','Uniatónoma del Cauca','Editorial Uniautónoma del Cauca','Sello Editorial Uniatónoma del Cauca'])
gruplac_caplibros['editorial']=gruplac_caplibros['editorial'].str.replace(pattern, 'Corporación Universitaria Autónoma del Cauca',regex=True)
pattern='|'.join(['SERVICIO NACIONAL DE APRENDIZAJE - SENA (Regional Cauca)'])
gruplac_caplibros['editorial']=gruplac_caplibros['editorial'].str.replace(pattern, 'SENA',regex=True)
gruplac_lineas['lineas']=gruplac_lineas['lineas'].replace({"1.\t":"","2.\t":"","^L.NEA .. ":""},regex=True)
gruplac_pdoctorado['fecha']=pd.to_datetime(gruplac_pdoctorado['fecha'])#Tiene valores nulos!
gruplac_pdoctorado['institucion']=gruplac_pdoctorado['institucion'].fillna('No Aplica')
col=gruplac_pdoctorado.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_pdoctorado.iloc[list(byinst_normalized.get_group(key).index),4]=gruplac_pdoctorado.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['programa']=col['programa'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
bydept_normalized=col.groupby('programa')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_pdoctorado.iloc[list(bydept_normalized.get_group(key).index),1]=gruplac_pdoctorado.iloc[list(bydept_normalized.get_group(key).index)]['programa'].value_counts().index[0]
bydept_normalized=0
gruplac_pmaestria['fecha']=pd.to_datetime(gruplac_pmaestria['fecha'])#Tiene valores nulos!
gruplac_pmaestria['institucion']=gruplac_pmaestria['institucion'].fillna('No Aplica')
col=gruplac_pmaestria.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_pmaestria.iloc[list(byinst_normalized.get_group(key).index),4]=gruplac_pmaestria.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['programa']=col['programa'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('programa')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_pmaestria.iloc[list(bydept_normalized.get_group(key).index),1]=gruplac_pmaestria.iloc[list(bydept_normalized.get_group(key).index)]['programa'].value_counts().index[0]
bydept_normalized=0
gruplac_cdoctorado['fecha']=pd.to_datetime(gruplac_cdoctorado['fecha'])#Tiene valores nulos![^a-zA-Z0-9]
col=gruplac_cdoctorado.copy()
col['programa']=col['programa'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byprog_normalized=col.groupby('programa')
for key in list(byprog_normalized.groups.keys()):  
    gruplac_cdoctorado.iloc[list(byprog_normalized.get_group(key).index),4]=gruplac_cdoctorado.iloc[list(byprog_normalized.get_group(key).index)]['programa'].value_counts().index[0]
byprog_normalized=0
col['curso']=col['curso'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('curso')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_cdoctorado.iloc[list(bydept_normalized.get_group(key).index),1]=gruplac_cdoctorado.iloc[list(bydept_normalized.get_group(key).index)]['curso'].value_counts().index[0]
bydept_normalized=0
gruplac_cmaestria['fecha']=pd.to_datetime(gruplac_cmaestria['fecha'])#Tiene valores nulos!
col=gruplac_cmaestria.copy()
col['programa']=col['programa'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byprog_normalized=col.groupby('programa')
for key in list(byprog_normalized.groups.keys()):  
    gruplac_cmaestria.iloc[list(byprog_normalized.get_group(key).index),4]=gruplac_cmaestria.iloc[list(byprog_normalized.get_group(key).index)]['programa'].value_counts().index[0]
byprog_normalized=0
col['curso']=col['curso'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('curso')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_cmaestria.iloc[list(bydept_normalized.get_group(key).index),1]=gruplac_cmaestria.iloc[list(bydept_normalized.get_group(key).index)]['curso'].value_counts().index[0]
bydept_normalized=0
gruplac_libros['lugar']=gruplac_libros['lugar'].fillna('No Aplica')
#remover  duplicados por (isbn limpio),idgruplac mantener el registro con mas columnas rellenas
gruplac_libros['fecha']=pd.to_datetime(gruplac_libros['fecha']).dt.to_period('Y')
gruplac_libros['editorial']=gruplac_libros['editorial'].fillna('No Aplica')
col=gruplac_libros.copy()
col['editorial']=col['editorial'].str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9]','', regex=True).str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip() 
byedit_normalized=col.groupby('editorial')
for key in list(byedit_normalized.groups.keys()):  
    gruplac_libros.iloc[list(byedit_normalized.get_group(key).index),7]=gruplac_libros.iloc[list(byedit_normalized.get_group(key).index)]['editorial'].value_counts().index[0]
byedit_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_libros.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_libros.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
pattern='|'.join(['Universidad del Cauca','Editorial Universidad Del Cauca','Ediorial Universidad Del Cauca','Sello Editorial Universidad del Cauca','Taller Editorial Universidad Del Cauca','Editorial De La Universidad Del Cauca','Univesidad Del Cauca','Talleres de impresión de la Universidad del Cauca','Talleres de Impresión la Universidad el Cauca','Centro De Publicaciones Universidad Del Cauca'])
gruplac_libros['editorial']=gruplac_libros['editorial'].str.replace(pattern, 'Editorial Universidad del Cauca',regex=True)
pattern='|'.join(['Sello editorial Uniautónoma del Cauca','Editorial de la Corporación Universitaria Autónoma del Cauca','Uniautónoma del Cauca','Uniatónoma del Cauca','Editorial Uniautónoma del Cauca','Sello Editorial Uniatónoma del Cauca'])
gruplac_libros['editorial']=gruplac_libros['editorial'].str.replace(pattern, 'Corporación Universitaria Autónoma del Cauca',regex=True)
pattern='|'.join(['SERVICIO NACIONAL DE APRENDIZAJE - SENA (Regional Cauca)'])
gruplac_libros['editorial']=gruplac_libros['editorial'].str.replace(pattern, 'SENA',regex=True)
gruplac_oarticulos['issn']=gruplac_oarticulos['issn'].replace('^0$','No Aplica',regex=True).fillna('No Aplica')
gruplac_oarticulos['fecha']=pd.to_datetime(gruplac_oarticulos['fecha']).dt.to_period('Y')
#remover duplicados por idgruplac,nombre,(paginas?) mantener el de mas columnas rellenas
gruplac_oarticulos['revista']=gruplac_oarticulos['revista'].fillna('No Aplica')
col=gruplac_oarticulos.copy()
col['revista']=col['revista'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
byrevi_normalized=col.groupby('revista')
for key in list(byrevi_normalized.groups.keys()):  
    gruplac_oarticulos.iloc[list(byrevi_normalized.get_group(key).index),5]=gruplac_oarticulos.iloc[list(byrevi_normalized.get_group(key).index)]['revista'].value_counts().index[0]
byrevi_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_oarticulos.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_oarticulos.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_olibros['lugar']=gruplac_olibros['lugar'].fillna('No Aplica')
gruplac_olibros['fecha']=pd.to_datetime(gruplac_olibros['fecha']).dt.to_period('Y')
#remover duplicados por idgruplac,(isbn limpio) y mantener eld e mas columnas rellenas
gruplac_olibros['editorial']=gruplac_olibros['editorial'].fillna('No Aplica')
col=gruplac_olibros.copy()
col['editorial']=col['editorial'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byedit_normalized=col.groupby('editorial')
for key in list(byedit_normalized.groups.keys()):  
    gruplac_olibros.iloc[list(byedit_normalized.get_group(key).index),9]=gruplac_olibros.iloc[list(byedit_normalized.get_group(key).index)]['editorial'].value_counts().index[0]
byedit_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_olibros.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_olibros.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
pattern='|'.join(['Universidad del Cauca','Editorial Universidad Del Cauca','Ediorial Universidad Del Cauca','Sello Editorial Universidad del Cauca','Taller Editorial Universidad Del Cauca','Editorial De La Universidad Del Cauca','Univesidad Del Cauca','Talleres de impresión de la Universidad del Cauca','Talleres de Impresión la Universidad el Cauca','Centro De Publicaciones Universidad Del Cauca'])
gruplac_olibros['editorial']=gruplac_olibros['editorial'].str.replace(pattern, 'Editorial Universidad del Cauca',regex=True)
pattern='|'.join(['Sello editorial Uniautónoma del Cauca','Editorial de la Corporación Universitaria Autónoma del Cauca','Uniautónoma del Cauca','Uniatónoma del Cauca','Editorial Uniautónoma del Cauca','Sello Editorial Uniatónoma del Cauca'])
gruplac_olibros['editorial']=gruplac_olibros['editorial'].str.replace(pattern, 'Corporación Universitaria Autónoma del Cauca',regex=True)
pattern='|'.join(['SERVICIO NACIONAL DE APRENDIZAJE - SENA (Regional Cauca)'])
gruplac_olibros['editorial']=gruplac_olibros['editorial'].str.replace(pattern, 'SENA',regex=True)
gruplac_disenoind['fecha']=pd.to_datetime(gruplac_disenoind['fecha']).dt.to_period('Y')
gruplac_innovaempresa['fecha']=pd.to_datetime(gruplac_innovaempresa['fecha']).dt.to_period('Y')
gruplac_innovaempresa['institucion']=gruplac_innovaempresa['institucion'].fillna('No Aplica')
col=gruplac_innovaempresa.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_innovaempresa.iloc[list(byinst_normalized.get_group(key).index),7]=gruplac_innovaempresa.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_innovaempresa.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_innovaempresa.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_plantapiloto['fecha']=pd.to_datetime(gruplac_plantapiloto['fecha']).dt.to_period('Y')
gruplac_plantapiloto['institucion']=gruplac_plantapiloto['institucion'].fillna('No Aplica')
col=gruplac_plantapiloto.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_plantapiloto.iloc[list(byinst_normalized.get_group(key).index),8]=gruplac_plantapiloto.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_plantapiloto.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_plantapiloto.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_otecnologicos['fecha']=pd.to_datetime(gruplac_otecnologicos['fecha']).dt.to_period('Y')
gruplac_otecnologicos['institucion']=gruplac_otecnologicos['institucion'].fillna('No Aplica')
col=gruplac_otecnologicos.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip()
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_otecnologicos.iloc[list(byinst_normalized.get_group(key).index),8]=gruplac_otecnologicos.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_otecnologicos.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_otecnologicos.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_prototipos['fecha']=pd.to_datetime(gruplac_prototipos['fecha']).dt.to_period('Y')
gruplac_prototipos['institucion']=gruplac_prototipos['institucion'].fillna('No Aplica')
col=gruplac_prototipos.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_prototipos.iloc[list(byinst_normalized.get_group(key).index),7]=gruplac_prototipos.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_prototipos.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_prototipos.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_software['disponibilidad']=gruplac_software['disponibilidad'].replace('Restrita','Restricta')
gruplac_software['fecha']=pd.to_datetime(gruplac_software['fecha']).dt.to_period('Y')
gruplac_software['institucion']=gruplac_software['institucion'].fillna('No Aplica')
col=gruplac_software.copy()
col['institucion']=col['institucion'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
byinst_normalized=col.groupby('institucion')
for key in list(byinst_normalized.groups.keys()):  
    gruplac_software.iloc[list(byinst_normalized.get_group(key).index),7]=gruplac_software.iloc[list(byinst_normalized.get_group(key).index)]['institucion'].value_counts().index[0]
byinst_normalized=0
col['nombre']=col['nombre'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(r'/\s+/g',' ',regex=True).str.replace(r'[^a-zA-Z0-9 ]','', regex=True).str.strip() 
bydept_normalized=col.groupby('nombre')
col=0
for key in list(bydept_normalized.groups.keys()):  
    gruplac_software.iloc[list(bydept_normalized.get_group(key).index),3]=gruplac_software.iloc[list(bydept_normalized.get_group(key).index)]['nombre'].value_counts().index[0]
bydept_normalized=0
gruplac_empresatec['fecha_registro']=pd.to_datetime(gruplac_empresatec['fecha_registro'])

#MANEJO DE DUPLICADOS
c=gruplac_articulos.groupby(['idgruplac','nombre','doi']).size().reset_index(name="size").sort_values('size',ascending=False)
d=gruplac_articulos.groupby(['idgruplac','nombre','revista','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
l=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
l.extend(d[d['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist())
gruplac_duplicados['articulos']=list(set(l))
d=0
l=0
#ARTICULOS DUPLICADOS EN GRUPLAC POR NOMBRE Y DOI
print("")
print("****PERFILES EN GRUPLAC CON ARTÍCULOS DUPLICADOS****")
print("Grupos con artículos duplicados en general: ",c[c['size']>1].drop_duplicates('idgruplac').shape[0])
c=gruplac_articulos[gruplac_articulos['verificado']=='True'].groupby(['idgruplac','nombre','doi']).size().reset_index(name="size").sort_values('size',ascending=False)
print("Grupos con artículos verificados duplicados: ",c[c['size']>1].drop_duplicates('idgruplac').shape[0])
articulos_totales=gruplac_articulos.shape[0]

gruplac_articulos=gruplac_articulos.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','doi'],keep='first')
gruplac_articulos=gruplac_articulos.drop_duplicates(['idgruplac','nombre','revista','fecha'],keep='first')

dup_arts=articulos_totales-gruplac_articulos.shape[0]
print("Artículos duplicados en los grupos del Cauca: ",dup_arts)

c=gruplac_oarticulos.groupby(['idgruplac','nombre','revista','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['oarticulos']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_oarticulos=gruplac_oarticulos.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','revista','fecha'],keep='first')
c=gruplac_libros.groupby(['idgruplac','isbn']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['libros']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_libros=gruplac_libros.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','isbn'],keep='first')
c=gruplac_olibros.groupby(['idgruplac','isbn']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['olibros']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_olibros=gruplac_olibros.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','isbn'],keep='first')
c=gruplac_caplibros.groupby(['idgruplac','capitulo','libro']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['caplibros']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_caplibros=gruplac_caplibros.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','capitulo','libro'],keep='first')
c=gruplac_otecnologicos.groupby(['idgruplac','nombre','fecha','institucion']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['otecnologicos']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_otecnologicos=gruplac_otecnologicos.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','fecha','institucion'],keep='first')
c=gruplac_software.groupby(['idgruplac','nombre','institucion']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['software']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_software=gruplac_software.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','institucion'],keep='first')
c=gruplac_prototipos.groupby(['idgruplac','nombre','institucion']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['prototipos']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_prototipos=gruplac_prototipos.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','institucion'],keep='first')
c=gruplac_cdoctorado.groupby(['idgruplac','curso','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['cdoctorado']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_cdoctorado=gruplac_cdoctorado.drop_duplicates(['idgruplac','curso','fecha'],keep='first')
c=gruplac_cmaestria.groupby(['idgruplac','curso','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['cmaestria']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_cmaestria=gruplac_cmaestria.drop_duplicates(['idgruplac','curso','fecha'],keep='first')
c=gruplac_innovaempresa.groupby(['idgruplac','nombre','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['innovaempresa']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_innovaempresa=gruplac_innovaempresa.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','fecha'],keep='first')
c=gruplac_empresatec.groupby(['idgruplac','nombre','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['empresatec']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_empresatec=gruplac_empresatec.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','fecha'],keep='first')
c=gruplac_plantapiloto.groupby(['idgruplac','nombre','fecha']).size().reset_index(name="size").sort_values('size',ascending=False)
gruplac_duplicados['empresatec']=c[c['size']>1].drop_duplicates('idgruplac')['idgruplac'].tolist()
gruplac_plantapiloto=gruplac_plantapiloto.sort_values(['verificado'],ascending=False).drop_duplicates(['idgruplac','nombre','fecha'],keep='first')

gruplac_articulos.to_csv('dashboard/assets/data/preprocessed_data/gruplac_articulos.csv',index=False)
gruplac_basico.to_csv('dashboard/assets/data/preprocessed_data/gruplac_basico.csv',index=False)
gruplac_caplibros.to_csv('dashboard/assets/data/preprocessed_data/gruplac_caplibros.csv',index=False)
gruplac_integrantes.to_csv('dashboard/assets/data/preprocessed_data/gruplac_integrantes.csv',index=False)
gruplac_libros.to_csv('dashboard/assets/data/preprocessed_data/gruplac_libros.csv',index=False)
gruplac_oarticulos.to_csv('dashboard/assets/data/preprocessed_data/gruplac_oarticulos.csv',index=False)
gruplac_olibros.to_csv('dashboard/assets/data/preprocessed_data/gruplac_olibros.csv',index=False)
gruplac_cdoctorado.to_csv('dashboard/assets/data/preprocessed_data/gruplac_cdoctorado.csv',index=False)
gruplac_cmaestria.to_csv('dashboard/assets/data/preprocessed_data/gruplac_cmaestria.csv',index=False)
gruplac_disenoind.to_csv('dashboard/assets/data/preprocessed_data/gruplac_disenoind.csv',index=False)
gruplac_empresatec.to_csv('dashboard/assets/data/preprocessed_data/gruplac_empresatec.csv',index=False)
gruplac_innovaempresa.to_csv('dashboard/assets/data/preprocessed_data/gruplac_innovaempresa.csv',index=False)
gruplac_instituciones.to_csv('dashboard/assets/data/preprocessed_data/gruplac_instituciones.csv',index=False)
gruplac_lineas.to_csv('dashboard/assets/data/preprocessed_data/gruplac_lineas.csv',index=False)
gruplac_otecnologicos.to_csv('dashboard/assets/data/preprocessed_data/gruplac_otecnologicos.csv',index=False)
gruplac_pdoctorado.to_csv('dashboard/assets/data/preprocessed_data/gruplac_pdoctorado.csv',index=False)
gruplac_plantapiloto.to_csv('dashboard/assets/data/preprocessed_data/gruplac_plantapiloto.csv',index=False)
gruplac_pmaestria.to_csv('dashboard/assets/data/preprocessed_data/gruplac_pmaestria.csv',index=False)
gruplac_prototipos.to_csv('dashboard/assets/data/preprocessed_data/gruplac_prototipos.csv',index=False)
gruplac_software.to_csv('dashboard/assets/data/preprocessed_data/gruplac_software.csv',index=False)


#ESTADÍSTICAS
print("")
print("Total de autores en Gruplac para el Cauca: ",gruplac_integrantes['url'].drop_duplicates().shape[0])
print("Grupos en Gruplac: ",gruplac_basico.shape[0])
print("")
gruplac_lineas_copy=gruplac_lineas.copy()
gruplac_lineas_copy['lineas']=gruplac_lineas_copy['lineas'].str.split(';')
gruplac_lineas_copy=gruplac_lineas_copy.explode('lineas')
gruplac_lineas_copy=gruplac_lineas_copy.groupby('lineas').size().reset_index(name='count').sort_values(by='count',ascending=False)
print('Total de líneas de investigación entre los grupos del Cauca: ',gruplac_lineas_copy.shape[0])
gruplac_lineas_copy=gruplac_lineas_copy[gruplac_lineas_copy['count']>1]
print('Total de líneas de investigación compartidas entre los grupos: ',gruplac_lineas_copy.shape[0])
print("**************PREPROCESAMIENTO FINALIZADO***************")



