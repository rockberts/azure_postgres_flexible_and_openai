----dataset origen viene de https://www.kaggle.com/code/fernandobordi/fb-experiencia-an-lisis-sentimientos
CREATE TABLE product_reviews_ori (
    Id SERIAL PRIMARY KEY,
    ProductId VARCHAR(50),
    UserId VARCHAR(50),
    ProfileName VARCHAR(100),
    HelpfulnessNumerator INTEGER,
    HelpfulnessDenominator INTEGER,
    Score INTEGER,
    Time BIGINT,
    Summary TEXT,
    Text TEXT
);


--\COPY product_reviews_ori (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, HelpfulnessDenominator, Score, Time, Summary, Text) FROM '/home/system/part_1.csv' WITH CSV HEADER ENCODING 'UTF8';

SHOW azure.extensions;



select name, version, comment from pg_available_extension_versions
where name in ('azure_ai', 'azure_local_ai', 'vector','postgis');


select * from product_reviews_ori limit 100;

CREATE TABLE product_reviews_peq AS (select * from product_reviews_ori limit 1000);

--busqueda por lenguaje natural
select * from product_reviews_peq
where text ILIKE '%Healthy, Pets, Food%'


--creamos un campo tipo tsvector 
ALTER TABLE product_reviews_peq ADD COLUMN textsearch tsvector
GENERATED ALWAYS AS (to_tsvector('english', summary || text)) STORED;

--búsqueda semantica
select * from product_reviews_peq
where textsearch @@phraseto_tsquery('%Healthy, Pets, Food%');

CREATE EXTENSION azure_ai;

--definimos endpoint de AOAI
select azure_ai.set_setting('azure_openai.endpoint','');
select azure_ai.set_setting('azure_openai.subscription_key','');

--probemos el embedding
select azure_openai.create_embeddings(deployment_name =>'text-embedding-3-large'
									  ,input =>'Falabella Ama Microsoft Azure'
									  ,dimensions =>384
									  ,timeout_ms =>1000
									  ,max_attempts =>2
									  ,retry_delay_ms =>500);

CREATE EXTENSION IF NOT EXISTS vector;

--PASAMOS A EMBEDDING DE AOAI LA CONCATENACION DE SUMMARY Y TEXT

ALTER TABLE product_reviews_peq ADD COLUMN desc_vector vector(384)
GENERATED ALWAYS AS (azure_openai.create_embeddings(deployment_name =>'text-embedding-3-large'
									  ,input => summary || text
									  ,dimensions =>384
									  ,timeout_ms =>1000
									  ,max_attempts =>2
									  ,retry_delay_ms =>500)::vector) STORED;
									 
--REVISAMOS
SELECT * FROM product_reviews_peq LIMIT 5;

--CREAMOS INDICES BEST PERF

CREATE INDEX products_embedding_cosine ON product_reviews_peq USING hnsw (desc_vector vector_cosine_ops); -- similaridad coseno
CREATE INDEX products_embedding_innerproduct ON product_reviews_peq USING hnsw (desc_vector vector_ip_ops); -- producto interno

									 

CREATE EXTENSION postgis;

--creamos una tabla para incluir puntos en santiago aleatoriamente 
CREATE TABLE posiciones_geograficas (
    ubicacion_geografica geography
);


DO $$
DECLARE
    lat_min float := -33.65;
    lat_max float := -33.35;
    lon_min float := -70.85;
    lon_max float := -70.50;
    lat float;
    lon float;
BEGIN
    FOR i IN 1..1000 LOOP
        lat := lat_min + random() * (lat_max - lat_min);
        lon := lon_min + random() * (lon_max - lon_min);
        EXECUTE format('INSERT INTO posiciones_geograficas (ubicacion_geografica) VALUES (ST_GeographyFromText(''SRID=4326;POINT(%s %s)''));', lon, lat);
    END LOOP;
END $$


SELECT * FROM posiciones_geograficas;

ALTER TABLE  product_reviews_peq DROP column IF EXISTS  ubicacion_geografica ;

ALTER TABLE  product_reviews_peq ADD COLUMN ubicacion_geografica geography;

--LE AGREGAMOS A LA TABLA DE PRODUCTO LAS POSICIONES ALEATOREAS DENTRO DE SANTIAGO

WITH posiciones AS (
    SELECT ubicacion_geografica, ROW_NUMBER() OVER () AS rn
    FROM public.posiciones_geograficas
),
tabla_existente AS (
    SELECT id, ROW_NUMBER() OVER () AS rn
    FROM public.product_reviews_peq
)
UPDATE public.product_reviews_peq te
SET ubicacion_geografica = p.ubicacion_geografica
FROM posiciones p
WHERE te.id = (SELECT id FROM tabla_existente WHERE rn = p.rn);


--VERIFICAR POSICIONES GEOGRAFICAS

SELECT ubicacion_geografica from product_reviews_peq;

--buscar comentarios de productos para mascotas a 7 km alrededor del mall alto las condes

WITH product_reviews_cte AS
(
	SELECT p.summary, p.text, p.desc_vector, p.ubicacion_geografica from product_reviews_peq p
	WHERE ST_DWithin(
		ubicacion_geografica::geography,
		ST_GeographyFromText('POINT(-70.54677820096387 -33.39059818345649)'), -- Ejemplo Falabella alto las condes
		7000 -- 10 km APROX
	)
)
SELECT text, summary, ubicacion_geografica, desc_vector from product_reviews_cte
ORDER BY desc_vector <=> azure_openai.create_embeddings('text-embedding-3-large',
				'comments pets food',384)::vector
LIMIT 50

-- ########################
-- prueba de servicios cognitivos => analisis de sentimientos

select azure_ai.set_setting('azure_cognitive.endpoint','');
select azure_ai.set_setting('azure_cognitive.subscription_key','');


-- analisis de sentimientos
select b.*, a.summary, a.text, a.productid from product_reviews_peq a,
azure_cognitive.analyze_sentiment(left(text,5000),'en') b
limit 10


--promedio de las reviews
with sentiment_cte as (
	select b.sentiment, b.positive_score, b.neutral_score, b.negative_score
	from product_reviews_peq a, azure_cognitive.analyze_sentiment(left(text,5000),'en') b
	limit 20
)
SELECT 
	sentiment, count(*) as ReviewCount,
	avg(positive_score) as avg_positive_score,
	avg(neutral_score) as avg_neutral_score,
	avg(negative_score) as avg_negative_score
FROM sentiment_cte s
group by sentiment


-- RESUMEN GENERADO POR AI
select azure_cognitive.summarize_abstractive(left(text,5000), 'en') as review_summary
FROM
(
	select text from product_reviews_peq limit 10
) as summarytable


-- EXTRAER FRASES CLAVES
SELECT a.summary, k.phrase
FROM product_reviews_peq a,
LATERAL (SELECT unnest(azure_cognitive.extract_key_phrases(a.summary, 'en')) AS phrase) k
LIMIT 10;


-- EXTRAER PII INFORMATION
select c.*, a.text from product_reviews_peq a, 
azure_cognitive.recognize_pii_entities(a.text) c
limit 5


--#########################
-- AHORA UN AZURE ML ENDPPOINT JSON PAYLOAD EXAMPLE

select azure_ai.set_setting('azure_ml.scoring_endpoint','');
select azure_ai.set_setting('azure_ml.endpoint_key', '');

select azure_ml.invoke(
'{
  "input_data": {
    "columns": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],
    "index": [0, 1],
    "data": [
            [20000,2,2,1,24,2,2,-1,-1,-2,-2,3913,3102,689,0,0,0,0,689,0,0,0,0],
            [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8]
        ]
  }
}', deployment_name=>'blue')


--##############################
-- AZURE LOCAL AI POSTGRES WITHOUT AOAI

CREATE EXTENSION azure_local_ai;



select * from azure_local_ai.create_embeddings(model_uri =>'multilingual-e5-small:v1',
												input =>'Falabella Ama Azure');
											
--agreguemos una columna para el embedding local
ALTER TABLE product_reviews_peq ADD COLUMN desc_vector_local vector(384)

--llenamos
UPDATE product_reviews_peq 
SET desc_vector_local = azure_local_ai.create_embeddings(
    model_uri => 'multilingual-e5-small:v1',
    input => summary || text
);


--probemos
select text, summary from  product_reviews_peq
order by desc_vector_local <=> azure_local_ai.create_embeddings(
				    model_uri => 'multilingual-e5-small:v1',
    				input =>'bad coffee ')::vector
limit 5;
	
--VER PERFORMANCE
explain analyze
	select azure_local_ai.create_embeddings('multilingual-e5-small:v1', summary) as embedding
	from product_reviews_peq
	limit 10;


explain analyze
	select azure_openai.create_embeddings('text-embedding-3-large', summary) as embedding
	from product_reviews_peq
	limit 10;
