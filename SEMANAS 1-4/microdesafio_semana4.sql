--1.CREA LA BASE DE DATOS DESASTRES

--CREATE DATABASE DESASTRES;

--2. CREA EL ESQUEMA DE ALMACENAMIENTO DE DATOS DE DESAST

CREATE SCHEMA ALMACENAMIENTO_DESASTRES;

--3.CREA LAS TABLA CLIMA FUTURO

CREATE TABLE clima
(año INT NOT NULL PRIMARY KEY,
Temperatura FLOAT NOT NULL,
Oxigeno FLOAT NOT NULL);

--4. INSERTA DATOS EN LA TABLA clima

INSERT INTO clima VALUES (2023, 22.5,230);
INSERT INTO clima VALUES (2024, 22.7,228.6);
INSERT INTO clima VALUES (2025, 22.9,227.5);
INSERT INTO clima VALUES (2026, 23.1,226.7);
INSERT INTO clima VALUES (2027, 23.2,226.4);
INSERT INTO clima VALUES (2028, 23.4,226.2);
INSERT INTO clima VALUES (2029, 23.6,226.1);
INSERT INTO clima VALUES (2030, 23.8,225.1);

--5. SELECT A LA TABLA clima

SELECT * FROM clima;

--6.CREA LA TABLA DESASTRES

CREATE TABLE desastres
(año INT NOT NULL PRIMARY KEY,
Tsunamis INT NOT NULL,
Olas_Calor INT NOT NULL,
Terremotos INT NOT NULL,
Erupciones INT NOT NULL,
Incendios INT NOT NULL);

--7. INSERTA LOS VALORES EN LA TABLA desastres

INSERT INTO desastres VALUES (2023, 2,15, 6,7,50);
INSERT INTO desastres VALUES (2024, 1,12, 8,9,46);
INSERT INTO desastres VALUES (2025, 3,16, 5,6,47);
INSERT INTO desastres VALUES (2026, 4,12, 10,13,52);
INSERT INTO desastres VALUES (2027, 5,12, 6,5,41);
INSERT INTO desastres VALUES (2028, 4,18, 3,2,39);
INSERT INTO desastres VALUES (2029, 2,19, 5,6,49);
INSERT INTO desastres VALUES (2030, 4,20, 6,7,50);

--8. SELECT A LA TABLA desastres

SELECT * FROM desastres;

--9. CREA LA TABLA muertes

CREATE TABLE muertes
(año INT NOT NULL PRIMARY KEY,
R_Menor15 INT NOT NULL,
R_15_a_30 INT NOT NULL,
R_30_a_45 INT NOT NULL,
R_45_a_60 INT NOT NULL,
R_M_a_60 INT NOT NULL);

--10. INSERTA LOS DATOS EN LA TABLA muertes

INSERT INTO muertes VALUES (2023, 1000,1300, 1200,1150,1500);
INSERT INTO muertes VALUES (2024, 1200,1250, 1260,1678,1940);
INSERT INTO muertes VALUES (2025, 987,1130, 1160,1245,1200);
INSERT INTO muertes VALUES (2026, 1560,1578, 1856,1988,1245);
INSERT INTO muertes VALUES (2027, 1002,943, 1345,1232,986);
INSERT INTO muertes VALUES (2028, 957,987, 1856,1567,1756);
INSERT INTO muertes VALUES (2029, 1285,1376, 1465,1432,1236);
INSERT INTO muertes VALUES (2030, 1145,1456, 1345,1654,1877);

--10. SELECT A LA TABLA muertes

SELECT * FROM muertes;

--11. CREA EL ESQUEMA DE INFORMES_DESASTRES

CREATE SCHEMA INFORMES_DESASTRES;

--12. CREA LA TABLA informe_desastre EN EL ESQUEMA INFORMES_DESASTRES

CREATE TABLE informe_desastre
(Cuatrenio varchar(20) NOT NULL PRIMARY KEY,
Temp_AVG FLOAT NOT NULL, Oxi_AVG FLOAT NOT NULL,
T_Tsunamis INT NOT NULL, T_OlasCalor INT NOT NULL,
T_Terremotos INT NOT NULL, T_Erupciones INT NOT NULL, 
T_Incendios INT NOT NULL,M_Jovenes_AVG FLOAT NOT NULL,
M_Adutos_AVG FLOAT NOT NULL,M_Ancianos_AVG FLOAT NOT NULL);

--13. CREA UN PROCEDURE QUE SE LLAME pETL_Desastres que permita cuantificar
--el cambio promedio en Temperatura y Oxigeno así cómo la suma total de otros
--eventos mencionados por cuatrienios

CREATE PROCEDURE pETL_Desastres()
LANGUAGE plpgsql
AS $$ 
  BEGIN
    DELETE FROM informe_desastre;
    INSERT INTO informe_desastre
    SELECT 
    CASE WHEN c.año BETWEEN 2023 AND 2026 THEN 'Q1(2023-2026'
    WHEN c.año BETWEEN 2027 AND 2030 THEN 'Q2(2027-2030)'
    ELSE 'ERROR'
    END as Cuatrienio,
    round(avg(c.Temperatura::numeric),2) as Temp_AVG ,round(avg(c.Oxigeno::numeric),2) as  Oxi_AVG,
    round(sum(d.Tsunamis::numeric),2) as T_Tsunamis,round(sum(d.Olas_Calor::numeric),2) as T_Olas_Calor ,
    round(sum(d.Terremotos::numeric),2) as T_Terremotos,round(sum(Erupciones::numeric),2) as T_Erupciones,
    round(sum(d.Incendios::numeric),2) as T_Incendios,round(avg(m.M_Jovenes::numeric),2) as M_Jovenes_AVG,
    round(avg(m.M_Adutos::numeric),2) as M_Adutos_AVG, round(avg(m.M_Adutos::numeric),2) as M_Adutos_AVG
    FROM clima c
    JOIN desastres d on d.año = c.año
    JOIN (SELECT año,R_Menor15+R_15_a_30 as M_Jovenes, R_30_a_45 + R_45_a_60 as M_Adutos, R_M_a_60 as M_Ancianos 
    FROM muertes) m on m.año = c.año
    GROUP BY 1
    ORDER BY 1;
  END
$$;

--14. Se llama al procedimiento

CALL pETL_Desastres();

--15. Consulta tabla desastres

SELECT * FROM informe_desastre;