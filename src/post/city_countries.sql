-- Remove city countries that do not exist in geo-db (fallen countries usually)
DELETE FROM cities_countries
WHERE NOT EXISTS(
	SELECT 1
	FROM countries
	WHERE countries.id = cities_countries.country
);

-- Pick one value for cities.country from cities_countries;
UPDATE cities
SET country = v_countries.country
FROM (
	SELECT cc.city, country FROM cities_countries
	INNER JOIN
	(
		SELECT city, MIN(priority) as prio
		FROM cities_countries
		GROUP BY city
	) cc
	ON cc.city = cities_countries.city
	AND cities_countries.priority = cc.prio
) AS v_countries
WHERE v_countries.city = cities.id;
