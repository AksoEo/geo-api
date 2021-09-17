DELETE FROM object_languages
WHERE
  lang IS NULL
  OR NOT EXISTS(
    SELECT 1
    FROM cities
    WHERE cities.id = object_languages.id
  );
