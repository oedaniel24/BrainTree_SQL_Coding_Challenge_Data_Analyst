--Data Integrity Checking & Cleanup (Bullet-Point 1)
WITH CountryCTE
AS
(
SELECT *
	, ROW_NUMBER() OVER(PARTITION BY country_code ORDER BY country_code) as RowNum
FROM
	continent_map
)
SELECT 
	COALESCE(country_code, 'FOO') as country_code
	, continent_code
FROM
	CountryCTE
WHERE
	RowNum > 1

--Data Integrity Checking & Cleanup (Bullet-Point 2)
WITH CountryCTE
AS
(
	SELECT *
		, ROW_NUMBER() OVER(PARTITION BY country_code ORDER BY country_code) as RowNum
	FROM
		continent_map
)
--creating temptable "continent_map_cleaned" with 1 record per country
SELECT 
	COALESCE(country_code, 'FOO') as country_code
	, continent_code
INTO continent_map_cleaned
FROM
	CountryCTE
WHERE RowNum = 1


-- List the countries ranked 10-12 in each continent by the percent of year-over-year growth descending from 2011 to 2012.
--create cte joining all the necessary columns 
WITH rankCTE
AS
(
	SELECT ROW_NUMBER() OVER(ORDER BY per_capita.country_code, per_capita.year) as [row_number]
		, per_capita.country_code as country_code
		, countries.country_name as country_name
		, continents.continent_name as continent_name
		, COALESCE(per_capita.gdp_per_capita, 0) as [gdp_per_capita_2011]
		, LEAD(COALESCE(per_capita.gdp_per_capita, 0)) OVER(ORDER BY per_capita.country_code, per_capita.year) as [gdp_per_capita_2012]
	FROM
		per_capita 
	LEFT JOIN
		continent_map_cleaned ON per_capita.country_code = continent_map_cleaned.country_code
	LEFT JOIN
		countries ON per_capita.country_code=countries.country_code
	LEFT JOIN 
		continents ON continent_map_cleaned.continent_code = continents.continent_code
	WHERE
		per_capita.year IN ('2011', '2012')
)
SELECT continent_name
	, country_code
	, country_name
	, gdp_per_capita_2011
	, gdp_per_capita_2012
	, CAST(((COALESCE((gdp_per_capita_2012-gdp_per_capita_2011)/NULLIF(gdp_per_capita_2011, 0),0)) * 100) AS NUMERIC(8,2)) as gr_percent           --numeric data type (order by this column)
	, CONCAT(CAST(((COALESCE((gdp_per_capita_2012-gdp_per_capita_2011)/NULLIF(gdp_per_capita_2011, 0),0)) * 100) AS NUMERIC(8,2)), '%') as growth_percent
INTO growth_percent_2011_2012
FROM 
	rankCTE 
WHERE 
	row_number % 2 <> 0
ORDER BY
	gr_percent DESC
-- assigning ranks
WITH growth_percent_CTE
AS
(
	SELECT RANK() OVER (PARTITION BY continent_name ORDER BY gr_percent DESC) as rank
		, continent_name
		, country_code
		, country_name
		, gdp_per_capita_2011
		, gdp_per_capita_2012
		, growth_percent
	FROM growth_percent_2011_2012
)
SELECT * FROM growth_percent_CTE WHERE rank IN (10, 11, 12) AND continent_name IS NOT NULL


-- For the year 2012, create a 3 column, 1 row report showing the percent share of gdp_per_capita for the following regions: 
-- (i) Asia (ii) Europe (iii) Rest of the world
WITH continetsGdp
AS
(
	SELECT
		[Asia]
		, [Europe]
		, [South America]
		, [North America]
		, [Oceania] 
		, [Antarctica] -- Antarctica is null so it's not included in the total column
		, ([Asia] + [Europe] + [South America] + [North America] + [Oceania]) as total
	FROM
	-- derived table
		(
		SELECT continents.continent_name as continent_name
			, per_capita.gdp_per_capita as gdp_per_capita
		FROM 
			continents
		LEFT JOIN 
			continent_map_cleaned ON continents.continent_code=continent_map_cleaned.continent_code
		LEFT JOIN 
			per_capita ON continent_map_cleaned.country_code=per_capita.country_code
		WHERE 
			per_capita.year = 2012
	) AS src
	-- using pivot function to transpose rows into columns
	PIVOT
	(
		SUM(gdp_per_capita)
		FOR [continent_name] IN ([Asia], [Europe], [South America], [North America], [Oceania], [Antarctica])
	) AS Pvt
)
-- selecting asia and europe and grouping other continents as rest of the world
SELECT
	CONCAT(CAST(([Asia] / total) * 100 as NUMERIC(10,2)), '%') as Asia
	, CONCAT(CAST(([Europe] / total) * 100 as NUMERIC(10,2)), '%') as Europe
	, CONCAT(CAST((([South America] + [North America] + [Oceania]) / total) * 100 as NUMERIC(10,2)), '%') as [Rest Of The World]
FROM continetsGdp


--What is the count of countries and sum of their related gdp_per_capita values for the year 2007 where the string 'an' (case insensitive)
--appears anywhere in the country name?
SELECT COUNT(countries.country_name) as [count of country]
	, CONCAT('$', CAST(SUM(per_capita.gdp_per_capita) AS NUMERIC(10,2))) as [total gdp_per_capita]
FROM 
	countries
INNER JOIN 
	per_capita ON countries.country_code=per_capita.country_code
WHERE
	per_capita.year = 2007 -- filtering the gdp per capita for just 2007
	AND
	-- filtering countries where the string 'an' (case insensitive) appears anywhere in the question name
	(country_name LIKE '%an'
	OR country_name LIKE '%an%'
	OR country_name LIKE 'an%')

-- case sensitive 'an'
SELECT COUNT(countries.country_name) as [count of country]
	, CONCAT('$',CAST(SUM(per_capita.gdp_per_capita) AS NUMERIC(10,2))) as [total gdp_per_capita]
FROM 
	countries
INNER JOIN 
	per_capita ON countries.country_code=per_capita.country_code
WHERE
	per_capita.year = 2007 -- filtering the gdp per capita for just 2007
	AND
	-- filtering countries where the string 'an' (case insensitive) appears anywhere in the question name 
	--by changing sql server case sensitivity using COLLATE
	(country_name LIKE '%an' COLLATE SQL_Latin1_General_CP1_CS_AS 
	OR country_name LIKE '%an%' COLLATE SQL_Latin1_General_CP1_CS_AS
	OR country_name LIKE 'an%' COLLATE SQL_Latin1_General_CP1_CS_AS)


-- 5. Find the sum of gpd_per_capita by year and the count of countries for each year that have non-null gdp_per_capita where
-- (i)the year is before 2012
SELECT year as year
	, COUNT(country_code) as country_count
	, CONCAT('$', CAST(SUM(gdp_per_capita) AS NUMERIC(20,2))) as total
FROM
	per_capita
WHERE 
	gdp_per_capita IS NOT NULL
AND
	year < 2012
GROUP BY year

-- (ii) the country has a null gdp_per_capita in 2012
SELECT year as year
	, COUNT(country_code) as country_count
	, CAST(SUM(gdp_per_capita) AS NUMERIC(20,2)) as total
FROM
	per_capita
WHERE 
	gdp_per_capita IS NULL
AND
	year = 2012
GROUP BY 
	year

-- Question 6
WITH running_total
AS
(
	SELECT
		continents.continent_code as continent_code
		, per_capita.country_code as country_code
		, countries.country_name as country_name
		, CONCAT('$', CAST(per_capita.gdp_per_capita AS NUMERIC(15,2))) as gdp_per_capita
	-- creating a running total of gdp_per_capita by continent_name
		, CAST(SUM(per_capita.gdp_per_capita) OVER(PARTITION BY continents.continent_code ORDER BY continents.continent_code, 
												SUBSTRING(countries.country_name, 2,3) DESC ROWS UNBOUNDED PRECEDING) AS NUMERIC(15,2)) as running
		, CONCAT('$', CAST(SUM(per_capita.gdp_per_capita) OVER(PARTITION BY continents.continent_code ORDER BY continents.continent_code, 
												SUBSTRING(countries.country_name, 2,3) DESC ROWS UNBOUNDED PRECEDING) AS NUMERIC(15,2)))
															as running_total_gdp_per_capita
	FROM 
		per_capita 
	LEFT JOIN 
		continent_map_cleaned ON per_capita.country_code = continent_map_cleaned.country_code
	LEFT JOIN 
		continents ON continent_map_cleaned.continent_code = continents.continent_code
	LEFT JOIN
		countries ON continent_map_cleaned.country_code = countries.country_code
	WHERE 
		continents.continent_code IS NOT NULL
)
--creating temptable ranking each country per continent starting from countries where running total of gdp_per_capita meets or exceeds $70,000.00
SELECT
	continent_code
	, country_code
	, country_name
	, gdp_per_capita
	, running_total_gdp_per_capita
	, RANK() OVER(PARTITION BY continent_code ORDER BY running) as [rank]
INTO 
	running_total_rank
FROM 
	running_total
where 
	running > 69999.99
-- returning only the first record from the ordered list
SELECT * FROM running_total_rank WHERE rank = 1



--  Country With The Highest Average Gdp_per_capita For Each Continent For All Years
-- creating CTE ranking countries by average gdp_per_capita
WITH avg_gdp_country
AS
(
	SELECT 
		RANK() OVER(PARTITION BY continents.continent_name ORDER BY AVG(per_capita.gdp_per_capita) DESC) as [rank]
		, continents.continent_name as continent_name
		, per_capita.country_code as country_code
		, countries.country_name as country_name
		, CONCAT('$', CAST(AVG(per_capita.gdp_per_capita) AS NUMERIC(10,2))) as avg_gdp_per_capita
	FROM 
		per_capita
	LEFT JOIN 
		continent_map_cleaned ON per_capita.country_code = continent_map_cleaned.country_code
	LEFT JOIN 
		continents ON continent_map_cleaned.continent_code = continents.continent_code
	LEFT JOIN 
		countries ON continent_map_cleaned.country_code = countries.country_code
	WHERE 
		continents.continent_name IS NOT NULL
	GROUP BY 
		continent_name, per_capita.country_code, country_name
)
-- returning top countries by avg gdp_per+capita for every continent
SELECT * FROM avg_gdp_country WHERE [rank] = 1


