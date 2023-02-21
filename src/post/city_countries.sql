# Remove city countries that do not exist in geo-db (fallen countries usually)
DELETE FROM cities_countries
WHERE NOT EXISTS(
	SELECT 1
	FROM countries
	WHERE countries.id = cities_countries.country
);

# Pick one value for cities.country from cities_countries;
UPDATE cities
SET country = v_countries.country
FROM (
	SELECT cities.id
	FROM cities
	INNER JOIN cities_countries
	ON cities_countries.city = cities.id
	ORDER BY cities_countries.priority
	LIMIT 1
) AS v_countries
WHERE v_countries.id = cities.id;
