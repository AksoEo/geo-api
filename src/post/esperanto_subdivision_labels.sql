ALTER TABLE cities ADD COLUMN "2nd_eo_label" string;

CREATE INDEX cities_2nd_eo_label_index ON cities ("2nd_eo_label");

UPDATE cities
SET "2nd_eo_label" = labels.label
FROM (
  SELECT
    DISTINCT cities."2nd_id",
    (
      SELECT label
	    FROM object_labels
      WHERE
        object_labels.id = cities."2nd_id"
        AND object_labels.lang IN ("eo","fr","es","en","de","nl")
	    ORDER BY object_labels.lang = "eo" DESC
	    LIMIT 1
    ) AS label
  FROM cities
) AS labels
WHERE cities.id = labels.id;
