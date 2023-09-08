
# Research Group Extractor

Software para la extracción de los datos bibliográficos de la producción científica de los investigadores y de grupos de investigación registrados en las plataformas CvLAC y GrupLAC pertenecientes a Scienti.

Tiene las siguientes características principales:
- Extrae datos de investigadores y grupos de investigación de las plataformas CvLAC y GrupLAC, de manera masiva o bajo demanda.
- Búsqueda y eliminación de documentos duplicados.
- Estadìsticas y limpieza de datos.
- Interfaz gráfica de usuario.

---

## Instalación
> [!IMPORTANT]
> ***Para el funcionamiento de la herramienta, es obligatorio tener instalado el gestor de base de datos [PostgreSQL.](https://www.postgresql.org/download/)*** *Si desea utilizar una herramienta gráfica para administrar PostgreSQL se recomienda instalar [PgAdmin 4.](https://www.pgadmin.org/download/)*

Para instalar el proyecto, realice una clonaciòn de la última versión del repositorio ejecutando el siguiente comando git:
```bash
    git clone https://github.com/JarbyDaniel/ScientiExtractor.git
```
Dentro de la carpeta generada, ejecute el siguiente comando para instalar las librerias necesarias:

```bash
    pip install -r requierements.txt
```

---

## Extractor de datos.

Esta herramienta extrae los datos del aplicativo CvLAC y GrupLAC pertenecientes al sistema Scienti en Colombia. Para utilizar esta herramienta, se requiere ingresar el enlace del CvLAC o GrupLAC de interés y la plataforma extraerá la información pública disponible en el sistema.
Se recomienda que cualquier persona que utilice esta herramienta lea detenidamente la [política de privacidad y los términos de uso](https://minciencias.gov.co/ciudadano/terminosycondiciones-datospersonales) definidos por MinCiencias, que establece las pautas y normas para el uso ético de los datos.

## Contexto

El extractor presentado forma parte de un software desarrollado dentro del marco del trabajo de grado titulado “Dashboard para el análisis y visualización bibliométrica dentro del ámbito de los grupos de investigación en el departamento del Cauca” desarrollado en la Universidad del Cauca por los estudiantes Edison Alexander Mosquera Perdomo y Jarby Daniel Salazar Galindez.

---

## Instrucciones para ejecutar el proyecto

Para extraer los datos de CvLAC y Gruplac de forma masiva (uso para administradores), ejecute el siguiente comado:

```bash
    python main.py    
```
Luego de la extracción, ejecute las estadísticas con el siguiente comando (se realizará automáticamente un proceso de limpieza previamente):

```bash
    python scienti_statistics.py
```
Para extraer los datos de CvLAC y Gruplac  bajo demanda (con interfaz de usuario), siga los siguientes pasos:

Para ejecutar la interfaz de usuario del extractor de CvLAC y Gruplac:
```bash
  python interfaz_extractor_scienti.py
```
Abra el siguiente link en el navegador: `127.0.0.1:5006`

---

Para verificar la consistencia entre los datos extraídos y los registrados en las plataformas de CvLAC y GrupLAC, ejecute el siguiente comando :

```bash
  python testing_main.py
```
Tenga en cuenta que la verificación se hace con respecto a los archivos CSV que debe generar manualmente, para ello reemplace y siga la estructura de los archivos para [CvLAC](cvlac/testing/testing_cvlac)  y [GrupLAC.](cvlac/testing/testing_gruplac)

---

> [!NOTE]
> Para expandir el uso del proyecto a otros departamentos de Colombia, utilice el [buscador de grupos por departamento](https://scienti.minciencias.gov.co/ciencia-war/BusquedaGrupoXDepartamento.do) de Minciencias para elegir el departamento de interés y reemplace la url utilizada en la línea 83 del archivo main.py

Ejemplo utilizado para el Departamento del Cauca:
```python
[83] lista_gruplac=Extractor.get_gruplac_list('https://scienti.minciencias.gov.co/ciencia-war/busquedaGrupoXDepartamentoGrupo.do?codInst=&sglPais=COL&sgDepartamento=CA&maxRows=15&grupos_tr_=true&grupos_p_=1&grupos_mr_=130')    
```
---

## Autores

- [Edison Alexander Mosquera Perdomo](https://www.github.com/alexper11)
- [Jarby Daniel Salazar Galindez](https://www.github.com/jarbydaniel)

## License

[MIT](https://choosealicense.com/licenses/mit/)
