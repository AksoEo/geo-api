DELETE FROM cities
WHERE NOT EXISTS(
  SELECT 1
  FROM countries
  WHERE countries.id = cities.country
);

UPDATE cities
SET country = countries.iso
FROM (
  SELECT id, iso
  FROM countries
) AS countries
WHERE cities.country = countries.id;

DROP TABLE countries;
