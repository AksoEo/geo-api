ALTER TABLE cities ADD COLUMN "2nd_native_label" string;

CREATE INDEX cities_2nd_native_label_index ON cities ("2nd_native_label");

UPDATE cities
SET "2nd_native_label" = labels.full_label
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
    labels.id = cities."2nd_id";
