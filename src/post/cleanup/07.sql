DELETE FROM object_labels
WHERE NOT EXISTS(
  SELECT 1
  FROM cities
  WHERE cities.id = object_labels.id
);
