ALTER TABLE cities ADD COLUMN native_label string;

CREATE INDEX cities_native_label_index ON cities (native_label);

UPDATE cities
SET native_label = labels.full_label
FROM (
  SELECT
    labels_inner.id,
    GROUP_CONCAT(labels_inner.label, " / ") AS full_label
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
  ) AS labels_inner
  GROUP BY labels_inner.id
) AS labels
WHERE
    labels.id = cities.id;
