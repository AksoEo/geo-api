ALTER TABLE cities ADD COLUMN "2nd_iso";

CREATE INDEX "cities_2nd_iso_index" ON cities ("2nd_iso");

UPDATE cities
SET "2nd_iso" = subdivisions.iso
FROM (
	SELECT id, iso
	FROM territorial_entities
	WHERE is_2nd
) AS subdivisions
WHERE subdivisions.id = cities."2nd_id";

