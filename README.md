Laura Arboleda Gallego, Giordan Jese Ricardo Parra

**INFRAESTRUCTURA DE BIG DATA**

EVIDENCIA DE APRENDIZAJE 1: Ingesta de una API
---------
**Descripción de la solución** 

El siguiente proyecto implementa un sistema de ingesta de datos de una API de Google Books, almacenándolos en una base de datos SQLite. Además, genera archivos de muestra en formato CSV y un archivo de auditoría en formato .txt para comparar los datos obtenidos de la API con los registros almacenados en la base de datos. La ejecución de los scripts está automatizada mediante GitHub Actions para garantizar la actualización y verificación de los datos. Aparte de esto, se toman los datos de la API para distribuirlos en diferentes tablas relacionadas entre sí, abriéndose a la posibilidad de realizar búsquedas y consultas a la API mediante datos organizados y en diferentes formatos, ya sea como una base de datos o un archivo CSV. 

**Instrucciones de uso y metodología**

**Paso 1 Crear repositorio:** Se procede a la creación de un nuevo repositorio en GitHub. Se debe crear bajo las siguientes condiciones: La primera es mantener el ReadMe y la segunda es ajustar el .gitignore configurado en Python. Esto último es muy importante pues cumple la función de que no se suban archivos innecesarios o con información sensible en el repositorio. Mejorando de paso la limpieza del repositorio. 

**Paso 2 Clonar repositorio:** Hay dos formas de clonar un repositorio. Si se desea hacer desde la terminal de Visual Studio en caso de trabajar en local, se abre la terminal y colocaremos lo siguiente: **git clone (inserta enlace del repositorio)** La segunda forma es hacerlo directamente en Visual Studio a través de las extensiones de git. La manera más segura en que se haga esto es a través de la terminal. 

**Paso 3 Antes de codificar, creamos los archivos principales:** Una vez con el repositorio clonado y la carpeta a nuestra disposición, comenzaremos a crear los primeros archivos. **Setup.py** el cual nos permitirá implementar las librerías que hemos de necesitar, estas siendo Pandas y openpyxl. Cabe resaltar que se agregan las librerías según las necesidades del proyecto. Luego, creamos el archivo **data.json** que es por donde se almacenan datos estructurados en formato JSON, funcionando también como un respaldo para evitar múltiples peticiones a la API. Por el momento vamos a mantener este archivo vacío de la siguiente forma **{ }**

**Paso 4 Creamos un entorno virtual:** Los entornos virtuales abren una brecha de versatilidad en el código, permitiendo una manipulación mucho más cómoda y eficaz. Ayudan a desacoplar y aislar las instalaciones de Python y los paquetes pip asociados. Para crear debemos abrir la terminal y usar el siguiente comando: **python -m venv venv** luego, procederemos con su activación, que se hace de la siguiente forma **venv\Scripts\activate** es **DE SUMA IMPORTANCIA** que el entorno virtual se cree dentro de la carpeta del repositorio, nunca por fuera. La terminal automáticamente nos ingresará a ese entorno. 

**Paso 5 Instalación de librerías y dependencias:** En el archivo setup.py vamos a tener las librerías a instalar dentro de **requires** por lo que, con el entorno virtual abierto vamos a ejecutar el siguiente comando: **pip install -e .** con esto las librerías comenzarán su instalación, una vez esté todo listo, ya podemos comenzar a trabajar en la ingesta. 

**Paso 6 Creación de carpetas:** En el siguiente orden crearemos las siguientes carpetas: **src** dentro de esta crearemos otra llamada **bigdata** dentro de esta vamos a crear una llamada **static** y dentro de **src** vamos a crear el archivo **ingesta.py** el cual será el archivo central que va a contener la funcionalidad y las conexiones que buscamos hacer. 

**Paso 7 Llamada a la API:** Se ejecuta el comando python src/bigdata/ingesta.py para el comienzo del procedo del archivo ingesta.py. Aqui haremos las conexiones con la API seleccionada, en este caso la de Google Books. Para esto debemos tener en cuenta varias cosas: Que se pueda ejecutar una lectura de los respectivos parámetros que se vinculen a la API y la URL de la misma. Los parámetros deben ajustarse en el código para que de esa manera cuando se ejecute pueda traer información. En este caso se consideraron parámetros de título de libros, un máximo de resultados (Pues se van a respaldar luego en una base de datos) y el cómo estos datos deben imprimirse, en este caso como libros. Si se cumple con esto, entonces ya se habría hecho una vinculación directa con la API, pasando al siguiente punto. 

**Paso 8 Almacenamiento a una base de datos:** Debido a que es un módulo directo de Python, sqlite3 es la mejor opción para almacenar los datos recolectados en la API. Antes de hacer cualquier conexión, primero debemos volver a crear una carpeta. Dentro de **bigdata** vamos a crear la carpeta **db** que es donde se va a almacenar la base de datos. Es de suma importancia ajustar el código de manera tal que la ruta donde se debe crear la base de datos se haga dentro de la carpeta, pues puede existir casos donde no reconozca la ruta o directamente cree el archivo directamente en la raíz del repositorio, lo que podría provocar problemas mayores cuando se quieran manipular estos datos. En este caso, nos tomamos la libertad de crear varias tablas según la información que trae la API: **books, authors, categories, books_authors y books_categories** estos dos últmos relacionándose a traves de datos del tipo id. es **IMPORTANTE** que sqlite3 esté previamente instalado no sólo dentro de Python sino también dentro de las variables de entorno del sistema operativo en caso de que se trabaje en local, en caso de estra trabajando en un servidor de GitHub por ejemplo, sólo se requerirían las extensiones. El código toma elementos de la API según los parámetros dados y llenan las tablas con esa información. Una vez se termina de ejecutar, las base de datos con formato .sqlite3 se hará de forma automática en la carpeta indicada.

**Paso 9 Conversion de la base de datos en un Dataframe y exportacion de este en archivos CSV y xlsx:** El proceso de exportación de datos comienza con la ejecución de una consulta SQL que extrae los registros almacenados en la base de datos SQLite. Esta consulta recupera los identificadores, títulos, descripciones y fechas de publicación de los libros, junto con los nombres de los autores asociados, concatenándolos en una sola columna para mejorar la legibilidad de los datos. La información obtenida se carga en un DataFrame de Pandas, lo que facilita su manipulación y su posterior exportación y auditoría. Si existen datos almacenados, el DataFrame se exporta en dos formatos: un archivo CSV (export_books.csv) y un archivo Excel (export_books.xlsx). Estos archivos se guardan en la ruta src/bigdata/static/xlsx/, permitiendo un acceso estructurado a la información exportada. En caso de que no haya datos insertados en la base de datos, el sistema imprimirá un mensaje indicando que no hay información disponible para exportar. Este procedimiento garantiza que los datos extraídos de la API y almacenados en la base de datos sean accesibles en múltiples formatos, facilitando su análisis y auditoría.

**Paso 10 Generacion de un archivo de texto para auditoria de los datos recolectados:** Para garantizar la trazabilidad de los datos, el sistema genera un archivo de auditoría en formato de texto, ubicado en src/bigdata/static/auditoria/auditoria.txt. Este archivo contiene información clave sobre la cantidad de registros en la base de datos antes y después del proceso de ingesta, permitiendo verificar la diferencia en los datos extraídos de la API y almacenados en el sistema. El proceso inicia con una consulta a la base de datos para contar el número de registros existentes antes de la ingesta. Luego, se ejecuta la extracción de datos desde la API de Google Books, insertando la información en la base de datos SQLite. Posteriormente, se vuelve a realizar una consulta para determinar la cantidad total de registros después de la inserción. La diferencia entre estos valores indica cuántos datos fueron añadidos en la ejecución del proceso. El archivo de auditoría almacena esta información de manera estructurada, permitiendo un seguimiento claro de la evolución de los datos almacenados. En cada ejecución del proceso de ingesta, este archivo se actualiza con nuevos registros, proporcionando evidencia de los cambios realizados en la base de datos y facilitando la detección de inconsistencias o problemas en la extracción de datos.

**Paso 11 Automatizacion con Github Actions para pruebas ante cambios:** Para garantizar la actualización y verificación de los datos, se ha configurado un workflow en GitHub Actions que se ejecuta automáticamente ante cualquier cambio en el repositorio. Este proceso instala Python y sus dependencias, ejecuta el script de ingesta de datos y verifica la correcta creación de la base de datos, así como la generación de los archivos de muestra y auditoría. Adicionalmente, el workflow almacena los archivos generados (ingesta.sqlite3, export_books.xlsx, export_books.csv y auditoria.txt) como artefactos en GitHub, permitiendo su descarga y análisis. Finalmente, se realiza un commit y push automático de los cambios, asegurando que cada actualización del repositorio refleje los nuevos datos extraídos y procesados.



