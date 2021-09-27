UPDATE cities
SET "2nd_native_label" = labels.full_label
FROM (
  SELECT
    DISTINCT cities."2nd_id",
    iif(label1.label IS NULL,
      iif(label2.label IS NULL,
	    NULL,
	    label2.label
	  ),
	  iif(label2.label IS NULL,
	    label1.label,
	    iif(label1.label = label2.label,
	      label1.label,
              label1.label || " / " || label2.label
	    )
	  )
    ) AS full_label
    
  FROM cities

  INNER JOIN countries
  ON countries.id = cities.country

  INNER JOIN object_languages ol1
  ON
    ol1.id = countries.id
    AND ol1.lang_index = 0

  INNER JOIN languages l1
  ON l1.id = ol1.lang_id

  LEFT JOIN object_languages ol2
  ON
    ol2.id = countries.id
    AND ol2.lang_index = 1

  LEFT JOIN languages l2
  ON l2.id = ol2.lang_id

  LEFT JOIN object_labels label1
  ON
    label1.id = cities."2nd_id"
    AND (
      label1.lang = l1.code
      OR label1.lang LIKE iif(instr(l1.code,"-") = 0, l1.code, substring(l1.code, 0, instr(l1.code,"-"))) || "-%"
    )
    
  LEFT JOIN object_labels label2
  ON
    l2.code NOT NULL
    AND label2.id = cities."2nd_id" 
    AND (
      label2.lang = l2.code
      OR label2.lang LIKE iif(instr(l2.code,"-") = 0, l2.code, substring(l2.code, 0, instr(l2.code,"-"))) || "-%"
    )

  WHERE
    cities."2nd_native_label" IS NULL
) AS labels

WHERE cities."2nd_id" = labels."2nd_id";
