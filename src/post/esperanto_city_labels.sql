ALTER TABLE cities ADD COLUMN eo_label string;

CREATE INDEX cities_eo_label_index ON cities (eo_label);

UPDATE cities
SET eo_label = labels.label
FROM (
  SELECT
    cities.id,
    (
      SELECT label
	    FROM object_labels
      WHERE
        object_labels.id = cities.id
        AND object_labels.lang IN ("eo","fr","es","en","de","nl")
	    ORDER BY object_labels.lang = "eo" DESC
	    LIMIT 1
    ) AS label
  FROM cities
) AS labels
WHERE id = labels.id;
