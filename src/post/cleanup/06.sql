DELETE FROM cities
WHERE
  native_label IS NULL
  AND eo_label IS NULL;
