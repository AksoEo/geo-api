use crossbeam::channel::Receiver;
use rusqlite::{params, Connection, Transaction};
use std::collections::VecDeque;

#[derive(Debug)]
pub enum DataEntry {
    TerritorialEntity {
        id: String,
        is_2nd: bool,
        iso: Option<String>,
    },
    TerritorialEntityParent {
        id: String,
        parent: String,
    },
    ObjectLanguage {
        id: String,
        lang_id: String,
        index: u32,
    },
    Language {
        id: String,
        code: String,
    },
    City {
        id: String,
        population: Option<u64>,
        lat: Option<f64>,
        lon: Option<f64>,
    },
    CityCountry {
        id: String,
        country: String,
        priority: u32,
    },
    ObjectLabel {
        id: String,
        lang: String,
        label: String,
        native_order: Option<u64>,
    },
    Country {
        id: String,
        iso: String,
    },
    MissingP17 {
        id: String,
    },
}

pub fn db_writer(out_file: &str, recv: Receiver<DataEntry>) -> rusqlite::Result<()> {
    debug!("Setting up database");
    let mut conn = Connection::open(out_file)?;

    conn.execute_batch(include_str!("setup.sql"))?;

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
                insert_entry(&tx, item)?;
            }
            tx.commit()?;
        }
    }

    if !item_buffer.is_empty() {
        let tx = conn.transaction()?;
        for item in item_buffer.drain(..) {
            insert_entry(&tx, item)?;
        }
        tx.commit()?;
    }

    Ok(())
}

fn insert_entry(tx: &Transaction, entry: DataEntry) -> rusqlite::Result<()> {
    match entry {
        DataEntry::TerritorialEntity { id, is_2nd, iso } => {
            tx.execute(
                "insert into territorial_entities (id, is_2nd, iso) values (?1, ?2, ?3)",
                params![id, is_2nd, iso],
            )?;
        }
        DataEntry::TerritorialEntityParent { id, parent } => {
            tx.execute(
                "insert into territorial_entities_parents (id, parent) values (?1, ?2) on conflict (id, parent) do nothing",
                params![id, parent],
            )?;
        }
        DataEntry::ObjectLanguage { id, lang_id, index } => {
            tx.execute(
                "insert into object_languages (id, lang_id, lang_index) values (?1, ?2, ?3) on conflict (id, lang_id) do nothing",
                params![id, lang_id, index],
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
            population,
            lat,
            lon,
        } => {
            tx.execute(
                "insert into cities (id, population, lat, lon) values (?1, ?2, ?3, ?4)",
                params![id, population, lat, lon],
            )?;
        }
        DataEntry::CityCountry {
            id,
            country,
            priority,
        } => {
            tx.execute(
                "insert or ignore into cities_countries (city, country, priority) values (?1, ?2, ?3)",
                params![id, country, priority],
            )?;
        }
        DataEntry::ObjectLabel {
            id,
            lang,
            label,
            native_order,
        } => {
            tx.execute(
                "insert into object_labels (id, lang, label, native_order) values (?1, ?2, ?3, ?4)",
                params![id, lang, label, native_order],
            )?;
        }
        DataEntry::Country { id, iso } => {
            tx.execute(
                "insert into countries (id, iso) values (?1, ?2)",
                params![id, iso],
            )?;
        }
        DataEntry::MissingP17 { id } => {
            tx.execute("insert into missing_p17 (id) values (?1)", params![id])?;
        }
    }
    Ok(())
}
