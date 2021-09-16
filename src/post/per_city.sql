UPDATE cities
SET native_label = group_concat(label, " / ")
FROM (
  SELECT DISTINCT label
  FROM (
    WITH RECURSIVE
      parents(step, id) AS (
        VALUES(0, ?1)

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
      object_labels.label
    FROM parents

    LEFT JOIN object_languages
    ON parents.id = object_languages.id

    INNER JOIN languages
    ON object_languages.lang_id = languages.id

    INNER JOIN object_labels ON
      object_labels.id = ?1
      AND (
        object_labels.lang = languages.code
        OR object_labels.lang LIKE iif(instr(languages.code,"-") = 0, languages.code, substring(languages.code, 0, instr(languages.code,"-"))) || "-%"
      )

    GROUP BY step, parents.id, object_languages.lang_id
    ORDER BY step ASC, lang_index ASC
    LIMIT 2
  )
)
WHERE id = ?1
