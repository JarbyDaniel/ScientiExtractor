from cvlac.ExtractorCvlac import ExtractorCvlac
from cvlac.ExtractorGruplac import ExtractorGruplac
from cvlac.util import get_lxml

import pandas as pd
import numpy as np
import os
import sys
import shutil

from cvlac.cvlac_models.DBmodel import create_cvlac_db
from cvlac.cvlac_controllers.ActuacionController import ActuacionController
from cvlac.cvlac_controllers.ArticulosController import ArticulosController
from cvlac.cvlac_controllers.BasicoController import BasicoController
from cvlac.cvlac_controllers.EvaluadorController import EvaluadorController
from cvlac.cvlac_controllers.IdentificadoresController import IdentificadoresController
from cvlac.cvlac_controllers.IdiomaController import IdiomaController
from cvlac.cvlac_controllers.InvestigacionController import InvestigacionController
from cvlac.cvlac_controllers.JuradosController import JuradosController
from cvlac.cvlac_controllers.LibrosController import LibrosController
from cvlac.cvlac_controllers.ReconocimientoController import ReconocimientoController
from cvlac.cvlac_controllers.RedesController import RedesController
from cvlac.cvlac_controllers.EstanciasController import EstanciasController
from cvlac.cvlac_controllers.AcademicaController import AcademicaController
from cvlac.cvlac_controllers.ComplementariaController import ComplementariaController
from cvlac.cvlac_controllers.EmpresaTecnologicaController import EmpresaTecnologicaController
from cvlac.cvlac_controllers.InnovacionEmpresarialController import InnovacionEmpresarialController
from cvlac.cvlac_controllers.CaplibrosController import CaplibrosController
from cvlac.cvlac_controllers.PrototipoController import PrototipoController
from cvlac.cvlac_controllers.SoftwareController import SoftwareController
from cvlac.cvlac_controllers.TecnologicosController import TecnologicosController
from cvlac.cvlac_controllers.MetaCvlacDBController import MetaCvlacDBController


from cvlac.gruplac_models.DBmodel import create_gruplac_db
from cvlac.gruplac_controllers.ArticulosGController import ArticulosGController
from cvlac.gruplac_controllers.BasicoGController import BasicoGController
from cvlac.gruplac_controllers.CaplibrosGController import CaplibrosGController
from cvlac.gruplac_controllers.CursoDoctoradoController import CursoDoctoradoController
from cvlac.gruplac_controllers.CursoMaestriaController import CursoMaestriaController
from cvlac.gruplac_controllers.DisenoIndustrialGController import DisenoIndustrialGController
from cvlac.gruplac_controllers.EmpresaTecnologicaGController import EmpresaTecnologicaGController
from cvlac.gruplac_controllers.InnovacionEmpresarialGController import InnovacionEmpresarialGController
from cvlac.gruplac_controllers.InstitucionesController import InstitucionesController
from cvlac.gruplac_controllers.IntegrantesController import IntegrantesController
from cvlac.gruplac_controllers.LibrosGController import LibrosGController
from cvlac.gruplac_controllers.LineasGController import LineasGController
from cvlac.gruplac_controllers.OtroProgramaController import OtroProgramaController
from cvlac.gruplac_controllers.OtrosArticulosController import OtrosArticulosController
from cvlac.gruplac_controllers.OtrosLibrosController import OtrosLibrosController
from cvlac.gruplac_controllers.OtrosTecnologicosController import OtrosTecnologicosController
from cvlac.gruplac_controllers.PlantaPilotoGController import PlantaPilotoGController
from cvlac.gruplac_controllers.ProgramaDoctoradoController import ProgramaDoctoradoController
from cvlac.gruplac_controllers.ProgramaMaestriaController import ProgramaMaestriaController
from cvlac.gruplac_controllers.PrototiposGController import PrototiposGController
from cvlac.gruplac_controllers.SoftwareGController import SoftwareGController
from cvlac.gruplac_controllers.MetaGruplacDBController import MetaGruplacDBController

             


if __name__ == '__main__':
    
    print("########### INICIO DE EXTRACCIÓN MASIVA DE DATOS ##########")
    sys.path.append(".")
    create_cvlac_db()
    create_gruplac_db()
    print("")
    print('Bases de datos creadas')
    
    ########################
    #MODULO CVLAC
    ########################
    
    Extractor=ExtractorGruplac()
    #para este caso el parametro de entrada es la url del buscador scienti para el departamento del Cauca
    lista_gruplac=Extractor.get_gruplac_list('https://scienti.minciencias.gov.co/ciencia-war/busquedaGrupoXDepartamentoGrupo.do?codInst=&sglPais=COL&sgDepartamento=CA&maxRows=15&grupos_tr_=true&grupos_p_=1&grupos_mr_=130')
    
    try:
        shutil.rmtree('data/extracted_data')
        os.mkdir('data/extracted_data')
    except Exception as e:
        print(e)
        pass
    
    ######################
    #Extraccion de tablas CVLAC
    ######################
    
    print('Setting grup attributes...')
    Extractor.set_grup_attrs(lista_gruplac)
    
    print('Updating cvlacdb...')
    
    basico=BasicoController()
    aux_basico=Extractor.grup_basico.drop_duplicates(ignore_index=True)
    aux_basico.to_csv('data/extracted_data/aux_basico.csv',index=False)
    basico.insert_df(aux_basico)
    del basico
    
    articulos=ArticulosController()
    aux_articulos=Extractor.grup_articulos.drop_duplicates(ignore_index=True)
    aux_articulos.to_csv('data/extracted_data/aux_articulos.csv',index=False)
    articulos.insert_df(aux_articulos)
    del articulos
    
    actuacion = ActuacionController()
    aux_actuacion=Extractor.grup_actuacion.drop_duplicates(ignore_index=True)
    aux_actuacion.to_csv('data/extracted_data/aux_actuacion.csv',index=False)
    actuacion.insert_df(aux_actuacion)
    del actuacion
    
    evaluador=EvaluadorController()
    evaluador.insert_df(Extractor.grup_evaluador.drop_duplicates(ignore_index=True))
    del evaluador
    
    identificadores=IdentificadoresController()
    aux_identificadores=Extractor.grup_identificadores.drop_duplicates(ignore_index=True)
    aux_identificadores.to_csv('data/extracted_data/aux_identificadores.csv',index=False)
    identificadores.insert_df(aux_identificadores)
    del identificadores
    
    idioma=IdiomaController()
    idioma.insert_df(Extractor.grup_idioma.drop_duplicates(ignore_index=True))
    del idioma
    
    investigacion=InvestigacionController()
    aux_investigacion=Extractor.grup_investiga.drop_duplicates(ignore_index=True)
    aux_investigacion.to_csv('data/extracted_data/aux_investigacion.csv',index=False)
    investigacion.insert_df(aux_investigacion)
    del investigacion
    
    jurados=JuradosController()
    jurados.insert_df(Extractor.grup_jurado.drop_duplicates(ignore_index=True))
    del jurados
    
    libros=LibrosController()
    aux_libros=Extractor.grup_libros.drop_duplicates(ignore_index=True)
    aux_libros.to_csv('data/extracted_data/aux_libros.csv',index=False)
    libros.insert_df(aux_libros)
    del libros
    
    reconocimiento=ReconocimientoController()
    aux_reconocimiento=Extractor.grup_reconocimiento.drop_duplicates(ignore_index=True)
    aux_reconocimiento.to_csv('data/extracted_data/aux_reconocimiento.csv',index=False)
    reconocimiento.insert_df(aux_reconocimiento)
    del reconocimiento
    
    redes=RedesController()
    aux_redes=Extractor.grup_redes.drop_duplicates(ignore_index=True)
    aux_redes.to_csv('data/extracted_data/aux_redes.csv',index=False)
    redes.insert_df(aux_redes)
    del redes
    
    estancias=EstanciasController()
    estancias.insert_df(Extractor.grup_estancias.drop_duplicates(ignore_index=True))
    del estancias
    
    academica=AcademicaController()
    academica.insert_df(Extractor.grup_academica.drop_duplicates(ignore_index=True))
    del academica
    
    complementaria=ComplementariaController()
    complementaria.insert_df(Extractor.grup_complementaria.drop_duplicates(ignore_index=True))
    del complementaria
    
    caplibros=CaplibrosController()
    aux_caplibros=Extractor.grup_caplibros.drop_duplicates(ignore_index=True)
    aux_caplibros.to_csv('data/extracted_data/aux_caplibros.csv',index=False)
    caplibros.insert_df(aux_caplibros)
    del caplibros
    
    empresatec=EmpresaTecnologicaController()
    aux_empresatec=Extractor.grup_empresa_tecnologica.drop_duplicates(ignore_index=True)
    aux_empresatec.to_csv('data/extracted_data/aux_empresatec.csv',index=False)
    empresatec.insert_df(aux_empresatec)
    del empresatec
    
    innovaempresa=InnovacionEmpresarialController()
    aux_innovaempresa=Extractor.grup_innovacion_empresarial.drop_duplicates(ignore_index=True)
    aux_innovaempresa.to_csv('data/extracted_data/aux_innovaempresa.csv',index=False)
    innovaempresa.insert_df(aux_innovaempresa)
    del innovaempresa
    
    prototipo=PrototipoController()
    aux_prototipo=Extractor.grup_prototipo.drop_duplicates(ignore_index=True)
    aux_prototipo.to_csv('data/extracted_data/aux_prototipo.csv',index=False)
    prototipo.insert_df(aux_prototipo)
    del prototipo
    
    software=SoftwareController()
    aux_software=Extractor.grup_software.drop_duplicates(ignore_index=True)
    aux_software.to_csv('data/extracted_data/aux_software.csv',index=False)
    software.insert_df(aux_software)
    del software
    
    tecnologicos=TecnologicosController()
    aux_tecnologicos=Extractor.grup_tecnologicos.drop_duplicates(ignore_index=True)
    aux_tecnologicos.to_csv('data/extracted_data/aux_tecnologicos.csv',index=False)
    tecnologicos.insert_df(aux_tecnologicos)
    del tecnologicos
    
    print('Extracción Cvlac Finalizada')
    
    ######################
    #Extraccion de tablas GRUPLAC
    ######################
    
    print('Setting perfil attributes')
    Extractor.set_perfil_attrs(lista_gruplac)
    
    print('Updating gruplacdb...')
    
    basicog=BasicoGController()
    aux_basicog=Extractor.perfil_basico
    aux_basicog.to_csv('data/extracted_data/aux_basicog.csv',index=False)
    basicog.insert_df(aux_basicog)
    del basicog
    
    articulosg=ArticulosGController()
    aux_articulosg=Extractor.perfil_articulos
    aux_articulosg.to_csv('data/extracted_data/aux_articulosg.csv',index=False)
    articulosg.insert_df(aux_articulosg)
    del articulosg
    
    instituciones=InstitucionesController()
    aux_instituciones=Extractor.perfil_instituciones
    aux_instituciones.to_csv('data/extracted_data/aux_instituciones.csv',index=False)
    instituciones.insert_df(aux_instituciones)
    del instituciones
    
    lineasg=LineasGController()
    aux_lineas=Extractor.perfil_lineas
    aux_lineas.to_csv('data/extracted_data/aux_lineas.csv',index=False)
    lineasg.insert_df(Extractor.perfil_lineas)
    del lineasg
    
    integrantes=IntegrantesController()
    aux_integrantes=Extractor.perfil_integrantes
    aux_integrantes.to_csv('data/extracted_data/aux_integrantes.csv',index=False)
    integrantes.insert_df(aux_integrantes)
    del integrantes
    
    pdoctorado=ProgramaDoctoradoController()
    aux_pdoctorado=Extractor.perfil_programa_doctorado
    aux_pdoctorado.to_csv('data/extracted_data/aux_pdoctorado.csv',index=False)
    pdoctorado.insert_df(aux_pdoctorado)
    del pdoctorado
    
    pmaestria=ProgramaMaestriaController()
    aux_pmaestria=Extractor.perfil_programa_maestria
    aux_pmaestria.to_csv('data/extracted_data/aux_pmaestria.csv',index=False)
    pmaestria.insert_df(aux_pmaestria)
    del pmaestria
    
    oprograma=OtroProgramaController()
    oprograma.insert_df(Extractor.perfil_otro_programa)
    del oprograma
    
    cdoctorado=CursoDoctoradoController()
    aux_cdoctorado=Extractor.perfil_curso_doctorado
    aux_cdoctorado.to_csv('data/extracted_data/aux_cdoctorado.csv',index=False)
    cdoctorado.insert_df(aux_cdoctorado)
    del cdoctorado
    
    cmaestria=CursoMaestriaController()
    aux_cmaestria=Extractor.perfil_curso_maestria
    aux_cmaestria.to_csv('data/extracted_data/aux_cmaestria.csv',index=False)
    cmaestria.insert_df(aux_cmaestria)
    del cmaestria
    
    librosg=LibrosGController()
    aux_librosg=Extractor.perfil_libros
    aux_librosg.to_csv('data/extracted_data/aux_librosg.csv',index=False)
    librosg.insert_df(aux_librosg)
    del librosg
    
    caplibrosg=CaplibrosGController()
    aux_caplibrosg=Extractor.perfil_caplibros
    aux_caplibrosg.to_csv('data/extracted_data/aux_caplibrosg.csv',index=False)
    caplibrosg.insert_df(aux_caplibrosg)
    del caplibrosg
    
    oarticulos=OtrosArticulosController()
    aux_oarticulos=Extractor.perfil_otros_articulos
    aux_oarticulos.to_csv('data/extracted_data/aux_oarticulos.csv',index=False)
    oarticulos.insert_df(aux_oarticulos)
    del oarticulos
    
    olibros=OtrosLibrosController()
    aux_olibros=Extractor.perfil_otros_libros
    aux_olibros.to_csv('data/extracted_data/aux_olibros.csv',index=False)
    olibros.insert_df(aux_olibros)
    del olibros
    
    disenoind=DisenoIndustrialGController()
    aux_disenoind=Extractor.perfil_diseno_industrial
    aux_disenoind.to_csv('data/extracted_data/aux_disenoind.csv',index=False)
    disenoind.insert_df(aux_disenoind)
    del disenoind
    
    otecnologicos=OtrosTecnologicosController()
    aux_otecnologicos=Extractor.perfil_otros_tecnologicos
    aux_otecnologicos.to_csv('data/extracted_data/aux_otecnologicos.csv',index=False)
    otecnologicos.insert_df(aux_otecnologicos)
    del otecnologicos
    
    prototiposg=PrototiposGController()
    aux_prototiposg=Extractor.perfil_prototipos
    aux_prototiposg.to_csv('data/extracted_data/aux_prototiposg.csv',index=False)
    prototiposg.insert_df(aux_prototiposg)
    del prototiposg
    
    softwareg=SoftwareGController()
    aux_softwareg=Extractor.perfil_software
    aux_softwareg.to_csv('data/extracted_data/aux_softwareg.csv',index=False)
    softwareg.insert_df(aux_softwareg)
    del softwareg
    
    empresatecg=EmpresaTecnologicaGController()
    aux_empresatecg=Extractor.perfil_empresa_tecnologica
    aux_empresatecg.to_csv('data/extracted_data/aux_empresatecg.csv',index=False)
    empresatecg.insert_df(aux_empresatecg)
    del empresatecg
    
    innovaempresag=InnovacionEmpresarialGController()
    aux_innovaempresag=Extractor.perfil_innovacion_empresarial
    aux_innovaempresag.to_csv('data/extracted_data/aux_innovaempresag.csv',index=False)
    innovaempresag.insert_df(aux_innovaempresag)
    del innovaempresag
    
    plantapilotog=PlantaPilotoGController()
    aux_plantapilotog=Extractor.perfil_planta_piloto
    aux_plantapilotog.to_csv('data/extracted_data/aux_plantapilotog.csv',index=False)
    plantapilotog.insert_df(aux_plantapilotog)
    del plantapilotog
    
    print('Extracción Gruplac Finalizada')
    
    del Extractor
    