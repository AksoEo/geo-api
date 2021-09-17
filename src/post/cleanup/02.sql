CREATE TABLE object_languages_tmp (
  "id"	string NOT NULL,
  "lang"	string,
  "lang_index"	integer NOT NULL,
  PRIMARY KEY("id","lang")
);
CREATE INDEX object_languages_new_lang_index ON object_languages_tmp (lang);
CREATE INDEX object_languages_new_lang_index_index ON object_languages_tmp (lang_index);

INSERT OR IGNORE
INTO object_languages_tmp
SELECT object_languages.id, languages.code AS lang, lang_index
FROM object_languages
LEFT JOIN languages
ON languages.id = object_languages.lang_id;

DROP TABLE object_languages;
ALTER TABLE object_languages_tmp RENAME TO object_languages;
