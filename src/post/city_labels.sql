ALTER TABLE cities ADD COLUMN native_label string;

CREATE INDEX cities_native_label_index ON cities (native_label);

UPDATE cities
SET native_label = GROUP_CONCAT(table_label.label, " / ")
FROM (
  SELECT
    DISTINCT label,
    c.id
  FROM cities c
  LEFT JOIN object_labels l
    ON c.id = l.id
  WHERE
    native_order NOT NULL
    AND native_order <= 1
) AS table_label
WHERE
    table_label.id = cities.id;
