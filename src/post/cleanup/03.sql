CREATE TABLE object_labels_tmp (
	"id"	string NOT NULL,
	"lang"	string NOT NULL,
	"label"	string NOT NULL,
	PRIMARY KEY("id","lang")
);
CREATE INDEX object_labels_new_lang_index ON object_labels_tmp (lang);
CREATE INDEX object_labels_new_label_index ON object_labels_tmp (label);

INSERT OR IGNORE
INTO object_labels_tmp
SELECT id, lang, label
FROM object_labels;

DROP TABLE object_labels;
ALTER TABLE object_labels_tmp RENAME TO object_labels;
