ALTER TABLE cities ADD COLUMN "2nd_id" string;
CREATE INDEX cities_2nd_id_index ON cities ("2nd_id");

UPDATE cities
SET "2nd_id" = data.parent
FROM (
  SELECT
    cities.id,
    (
      SELECT parents_outer.id
      FROM (
        WITH RECURSIVE parents(step, id) AS (
          VALUES(0, cities.id)
          UNION ALL
          SELECT
          step + 1 as step,
          parent AS id
          FROM territorial_entities_parents, parents
          WHERE
            territorial_entities_parents.id = parents.id
            AND step < 100
        )
        SELECT
          step,
          id
        FROM parents
      ) AS parents_outer
      INNER JOIN territorial_entities t
      ON t.id = parents_outer.id
      WHERE is_2nd
      ORDER BY step DESC
    ) AS parent
  FROM cities
) data
WHERE cities.id = data.id;
