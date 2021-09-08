use crossbeam::channel::Receiver;
use rusqlite::{params, Connection};
use std::collections::VecDeque;

#[derive(Debug)]
pub enum DataEntry {
    TerritorialEntity {
        id: String,
    },
    TerritorialEntityParent {
        id: String,
        parent: String,
    },
    ObjectLanguage {
        id: String,
        lang_id: String,
    },
    Language {
        id: String,
        code: String,
    },
    City {
        id: String,
        country: String,
        population: Option<u64>,
        lat: Option<f64>,
        lon: Option<f64>,
    },
    CityLabel {
        id: String,
        lang: String,
        label: String,
        native_order: u64,
    },
    Country {
        id: String,
        iso: String,
    },
}

pub fn db_writer(recv: Receiver<DataEntry>) -> rusqlite::Result<()> {
    debug!("Setting up database");
    let mut conn = Connection::open("./geo.db")?;

    // since we are just writing to a blank database,
    // we can tune sqlite for speed at the expense of safety
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
            PRAGMA cache_size = 100000;
            PRAGMA journal_mode = MEMORY;
            PRAGMA temp_store = MEMORY;",
    )?;

    // TODO: add indexes

    conn.execute(
        "create table if not exists countries (
                id string not null primary key,
                iso char(2) not null)",
        [],
    )?;
    conn.execute(
        "create index if not exists countries_iso_index on countries (iso)",
        [],
    )?;
    conn.execute(
        "create table if not exists object_languages (
                id string not null,
                lang_id string not null,
                primary key (id, lang_id))",
        [],
    )?;
    conn.execute(
        "create table if not exists languages (
                id string not null,
                code string not null,
                primary key (id, code))",
        [],
    )?;
    conn.execute(
        "create index if not exists languages_code_index on languages (code)",
        [],
    )?;
    conn.execute(
        "create table if not exists territorial_entities (
                id string not null primary key)",
        [],
    )?;
    conn.execute(
        "create table if not exists territorial_entities_parents (
                id string not null,
                parent string not null,
                primary key (id, parent))",
        [],
    )?;
    conn.execute(
        "create index if not exists territorial_entities_parents_parent_index on territorial_entities_parents (parent)",
        [],
    )?;
    conn.execute(
        "create table if not exists cities (
                id string not null primary key,
                country string not null,
                population integer,
                lat decimal(5, 3),
                lon decimal(6, 3))",
        [],
    )?;
    conn.execute(
        "create table if not exists cities_labels (
                id string not null,
                lang string not null,
                native_order integer not null,
                label string not null,
                primary key (id, lang, native_order))",
        [],
    )?;
    conn.execute(
        "create index if not exists cities_labels_lang_index on cities_labels (lang)",
        [],
    )?;
    conn.execute(
        "create index if not exists cities_labels_native_order_index on cities_labels (native_order)",
        [],
    )?;

    debug!("Database set up");

    let mut item_buffer = VecDeque::with_capacity(128);
    loop {
        let item = match recv.recv() {
            Ok(item) => item,
            Err(e) => {
                debug!("closing DB writer because channel was disconnected: {}", e);
                break;
            }
        };

        item_buffer.push_back(item);

        if item_buffer.len() >= 127 {
            let tx = conn.transaction()?;

            for item in item_buffer.drain(..) {
                match item {
                    DataEntry::TerritorialEntity { id } => {
                        tx.execute(
                            "insert into territorial_entities (id) values (?1)",
                            params![id],
                        )?;
                    }
                    DataEntry::TerritorialEntityParent { id, parent } => {
                        tx.execute(
                            "insert into territorial_entities_parents (id, parent) values (?1, ?2) on conflict (id, parent) do nothing",
                            params![id, parent],
                        )?;
                    }
                    DataEntry::ObjectLanguage { id, lang_id } => {
                        tx.execute(
                            "insert into object_languages (id, lang_id) values (?1, ?2) on conflict (id, lang_id) do nothing",
                            params![id, lang_id],
                        )?;
                    }
                    DataEntry::Language { id, code } => {
                        tx.execute(
                            "insert into languages (id, code) values (?1, ?2)",
                            params![id, code],
                        )?;
                    }
                    DataEntry::City {
                        id,
                        country,
                        population,
                        lat,
                        lon,
                    } => {
                        tx.execute(
                            "insert into cities (id, country, population, lat, lon) values (?1, ?2, ?3, ?4, ?5)",
                            params![id, country, population, lat, lon],
                        )?;
                    }
                    DataEntry::CityLabel {
                        id,
                        lang,
                        label,
                        native_order,
                    } => {
                        tx.execute(
                            "insert into cities_labels (id, lang, label, native_order) values (?1, ?2, ?3, ?4)",
                            params![id, lang, label, native_order],
                        )?;
                    }
                    DataEntry::Country { id, iso } => {
                        tx.execute(
                            "insert into countries (id, iso) values (?1, ?2)",
                            params![id, iso],
                        )?;
                    }
                }
            }

            tx.commit()?;
        }
    }

    Ok(())
}
