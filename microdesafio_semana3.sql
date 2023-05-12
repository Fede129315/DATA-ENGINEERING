--0.Crear la base de datos BDE
--CREATE database BDE;
--1.Crear el esquema articulos en la base de datos BDE 

CREATE SCHEMA articulos;

--2.crea la tabla titulos dentro del esquema articulos

CREATE TABLE articulos.titulos (
id int primary key,
titulo varchar(50) NOT NULL,
tipo varchar(50) NOT NULL,
autor_id int);

--3.Inserta datos en la tabla titulos dentro del esquema articulos

INSERT INTO articulos.titulos VALUES (1,'EL REY LEON','INFANTIL',1);
INSERT INTO articulos.titulos VALUES (2,'TERMINATOR','CIENCIA FICCIÓN',2);
INSERT INTO articulos.titulos VALUES (3,'ALADIN','INFANTIL',3);
INSERT INTO articulos.titulos VALUES (4,'CHUKY','TERROR',4);

--4. Consulta tabla de titulos

SELECT * FROM articulos.titulos;

-- 5.Crear tabla autores

CREATE TABLE articulos.autores (
id int primary key,
nombreAutor varchar(50) NOT NULL,
apellidoAutor varchar(50) NOT NULL,
paisOrigenAutor varchar(50) NOT NULL);

--6.Insertar datos articulos

INSERT INTO articulos.autores VALUES (1,'PEPITO','PEREZ','ARGENTINA');
INSERT INTO articulos.autores VALUES (2,'JUAN','GONZALEZ','ESPAÑA');
INSERT INTO articulos.autores VALUES (3,'NORMA','ALEANDRO','ITALIA');
INSERT INTO articulos.autores VALUES (4,'RAUL','BERNAL','MEXICO');

--7. Consulta tabla de autores

SELECT * FROM articulos.autores;

--8.Crear la base de datos BDE_DW (datewarehouse)
--create database BDE_DW, en lugar de eso creo otro esquema para simular

CREATE SCHEMA informes;

--9.Crear la tabla dimTitulo para el informe

CREATE TABLE informes.dimTitulo(
titulo varchar(50),
tipo varchar(50),
nombreCompleto varchar(100),
paisOrigenAutor varchar(50));

--10. Crear procedimiento de llenado para ETL

CREATE PROCEDURE pETL_Insertar_dimTitulo()
LANGUAGE plpgsql
AS $$ 
BEGIN
DELETE FROM informes.dimTitulo;
INSERT INTO informes.dimTitulo 
SELECT t.titulo, t.tipo, CONCAT(a.nombreAutor,' ',a.apellidoAutor) as nombreCompleto, a.paisOrigenAutor
FROM articulos.titulos t  
JOIN articulos.autores a ON t.autor_id = a.id;
END
$$;

--11. Se llama al procedimiento
CALL pETL_Insertar_dimTitulo();

--12. Consulta tabla de autores

SELECT * FROM informes.dimTitulo;


