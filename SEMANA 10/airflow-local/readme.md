__Para poder ejectutar el Dag:__    
1- En el directorio airflow-local colo un archivo .env con las credenciales aportadas (para el caso de windows).   
2- Crear ".env" dentro del directorio plugins con las credenciales de Redshift  
3- Correr el siguiente comando parado en la carpeta principal donde se encuentra el docker-compose  
~~~
docker-compose up airflow-init
~~~
4- Una vez finalizado, correr el siguiente comando parado en la carpeta principal donde se encuentra el docker-compose  
~~~
docker-compose build
~~~
5- Una vez finalizado, correr el siguiente comando parado en la carpeta principal donde se encuentra el docker-compose  
~~~
docker-compose up
~~~
6- Desde un explorador, abrir http://localhost:8080/ y colocar credenciales para acceder  
7- Ejecutar el Dag  
