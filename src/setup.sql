-- since we are just writing to a blank database,
-- we can tune sqlite for speed at the expense of safety
PRAGMA synchronous = OFF;
PRAGMA cache_size = 100000;
PRAGMA journal_mode = MEMORY;
PRAGMA temp_store = MEMORY;

create table countries (
        id string not null primary key,
        iso char(2) not null
);
create index countries_iso_index on countries (iso);

create table object_languages (
        id string not null,
        lang_id string not null,
        lang_index integer not null,
        primary key (id, lang_id)
);
create index object_languages_lang_id_index on object_languages (lang_id);

create table languages (
        id string not null primary key,
        code string not null);
create index languages_code_index on languages (code);

create table territorial_entities (
    id string not null primary key,
    is_2nd boolean not null
);
create index territorial_entities_is_2nd on territorial_entities (is_2nd);

create table territorial_entities_parents (
    id string not null,
    parent string not null,
    primary key (id, parent)
);
create index territorial_entities_parents_parent_index on territorial_entities_parents (parent);

create table cities (
    id string not null primary key,
    country string not null,
    population integer,
    lat decimal(5, 3),
    lon decimal(6, 3)
);
create index cities_country_index on cities (country);
create index cities_population_index on cities (population);
create index cities_lat_index on cities (lat);
create index cities_lon_index on cities (lon);

create table object_labels (
    id string not null,
    lang string not null,
    native_order integer,
    label string not null,
    primary key (id, lang, native_order)
);
create index object_labels_label_index on object_labels (label);
create index object_labels_lang_index on object_labels (lang);
create index object_labels_native_order_index on object_labels (native_order);

create table missing_p17 (id string not null primary key);
