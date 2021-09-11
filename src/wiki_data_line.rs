use crate::database::DataEntry;
use crate::json_get;
use crate::wiki_sparql::Classes;
use crate::wiki_time::{is_object_active, parse_wikidata_time};
use crossbeam::channel::Sender;
use serde_json::Value;
use std::collections::HashSet;
use thiserror::Error;

fn handle_territorial_entity(obj: &Value, sink: &Sender<DataEntry>) -> Result<(), HandleLineError> {
    let obj_id = json_get!(value(obj).id: string).unwrap();
    sink.send(DataEntry::TerritorialEntity { id: obj_id.into() })?;

    if let Some(parents) = json_get!(value(obj).claims.P131: array) {
        for parent in parents {
            if !is_object_active(json_get!(value(parent).qualifiers: object)) {
                continue;
            }

            if let Some(parent) = json_get!(value(parent).mainsnak.datavalue.value.id: string) {
                sink.send(DataEntry::TerritorialEntityParent {
                    id: obj_id.into(),
                    parent: parent.into(),
                })?;
            } else {
                debug!(
                    "skipping TE {} P131 parent because it has no datavalue ID",
                    obj_id
                );
            }
        }
    }

    if let Some(langs) = json_get!(value(obj).claims.P37: array) {
        for lang in langs {
            // official language
            if json_get!(value(lang).mainsnak.snaktype: string) != Some("value") {
                continue;
            }
            if !is_object_active(json_get!(value(lang).qualifiers: object)) {
                continue;
            }
            if let Some(lang_id) = json_get!(value(lang).mainsnak.datavalue.value.id: string) {
                sink.send(DataEntry::ObjectLanguage {
                    id: obj_id.into(),
                    lang_id: lang_id.into(),
                })?;
            } else {
                debug!(
                    "skipping TE {} P37 lang because it has no datavalue ID",
                    obj_id
                );
            }
        }
    }
    Ok(())
}

fn handle_language(obj: &Value, sink: &Sender<DataEntry>) -> Result<(), HandleLineError> {
    let obj_id = json_get!(value(obj).id: string).unwrap();
    if let Some(wikimedia_code) =
        json_get!(value(obj).claims.P424[0].mainsnak.datavalue.value: string)
    {
        sink.send(DataEntry::Language {
            id: obj_id.into(),
            code: wikimedia_code.into(),
        })?;
    } else {
        // debug!("skipping lang {} because it has no wikimedia language code", obj_id);
    }
    Ok(())
}

fn handle_human_settlement(obj: &Value, sink: &Sender<DataEntry>) -> Result<(), HandleLineError> {
    let obj_id = json_get!(value(obj).id: string).unwrap();
    let country_entries = match json_get!(value(obj).claims.P17: array) {
        Some(country_entries) => country_entries,
        None => {
            sink.send(DataEntry::MissingP17 { id: obj_id.into() })?;
            return Ok(()); // we cannot use the entry without its country
        }
    };

    let mut country_id = None;
    for country_entry in country_entries {
        if is_object_active(json_get!(value(country_entry).qualifiers: object)) {
            if let Some(id) = json_get!(value(country_entry).mainsnak.datavalue.value.id: string) {
                country_id = Some(id.to_string());
            } else {
                debug!(
                    "skipping HS {} P17 country entry because it has no datavalue id",
                    obj_id
                );
            }
        }
    }

    let mut population = None;
    let mut population_time = None;
    if let Some(population_entries) = json_get!(value(obj).claims.P1082: array) {
        for population_entry in population_entries {
            let mut new_population_time = None;
            if let Some(population_time) =
                json_get!(value(population_entry).qualifiers.P585[0]: object)
            {
                if json_get!((population_time).snaktype: string) != Some("value") {
                    continue;
                }
                if let Some(time_obj) = json_get!((population_time).datavalue.value: object) {
                    if let (Some(time), Some(zone)) = (
                        json_get!((time_obj).time: string),
                        json_get!((time_obj).timezone: number),
                    ) {
                        if let Ok(time) = parse_wikidata_time(time, zone) {
                            new_population_time = Some(time);
                        }
                    } else {
                        debug!(
                            "skipping {} P1082/P585 population entry because it has invalid time",
                            obj_id
                        );
                    }
                } else {
                    debug!(
                        "skipping {} P1082/P585 population entry because it has no time value",
                        obj_id
                    );
                }
            } else {
                // debug!("skipping {} P1082 population entry because it has no P585 entry", obj_id);
            }

            if let Some(new_time) = new_population_time {
                if population_time
                    .as_ref()
                    .map_or(true, |old| new_time >= *old)
                {
                    if let (Some(value), Some(unit)) = (
                        json_get!(value(population_entry).mainsnak.datavalue.value.amount: string),
                        json_get!(value(population_entry).mainsnak.datavalue.value.unit: string),
                    ) {
                        // wikidata population is stored as "value" and "unit" strings
                        // I don't know what unit does
                        if unit != "1" {
                            warn!("Skipping {} P1082 population entry because I don't know what unit != 1 does", obj_id);
                            continue;
                        }

                        if let Ok(value) = value.parse() {
                            population = Some(value);
                            population_time = Some(new_time);
                        } else {
                            warn!("skipping {} P1082 population entry because its amount value could not be parsed to u64", obj_id);
                        }
                    } else {
                        debug!("skipping {} P1082 population entry because its amount value is an unexpected type", obj_id);
                    }
                }
            }
        }
    }

    let mut lat_lon = None;
    if let Some(coords) = json_get!(value(obj).claims.P625[0].mainsnak: object) {
        if json_get!((coords).snaktype: string) == Some("value") {
            if let (Some(lat), Some(lon)) = (
                json_get!((coords).datavalue.value.latitude: number),
                json_get!((coords).datavalue.value.longitude: number),
            ) {
                lat_lon = Some((lat, lon));
            } else {
                debug!(
                    "skipping {} lat/lon because lat/lon are invalid types",
                    obj_id
                );
            }
        }
    } else {
        // debug!("skipping {} lat/lon because it has no P625 entry", obj_id);
    }

    if let Some(country_id) = country_id {
        sink.send(DataEntry::City {
            id: obj_id.into(),
            country: country_id,
            population,
            lat: lat_lon.map(|(lat, _)| lat),
            lon: lat_lon.map(|(_, lon)| lon),
        })?;
    }

    if let Some(labels) = json_get!(value(obj).labels: object) {
        for label in labels.values() {
            if let (Some(lang), Some(label)) = (
                json_get!(value(label).language: string),
                json_get!(value(label).value: string),
            ) {
                sink.send(DataEntry::CityLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: None,
                })?;
            } else {
                debug!("skipping {} label because it has invalid type", obj_id);
            }
        }
    }

    // Insert native labels
    let mut native_order_index = 0;
    if let Some(native_labels) = json_get!(value(obj).claims.P1705: array) {
        for claim in native_labels {
            if let (Some(lang), Some(label)) = (
                json_get!(value(claim).mainsnak.datavalue.value.language: string),
                json_get!(value(claim).mainsnak.datavalue.value.text: string),
            ) {
                sink.send(DataEntry::CityLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: Some(native_order_index),
                })?;
                native_order_index += 1;
            } else {
                debug!(
                    "skipping {} P1705 native label because it has invalid type",
                    obj_id
                );
            }
        }
    }
    if let Some(official_names) = json_get!(value(obj).claims.P1448: array) {
        for claim in official_names {
            if !is_object_active(json_get!(value(claim).qualifiers: object)) {
                continue;
            }
            if let (Some(lang), Some(label)) = (
                json_get!(value(claim).mainsnak.datavalue.value.language: string),
                json_get!(value(claim).mainsnak.datavalue.value.text: string),
            ) {
                sink.send(DataEntry::CityLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: Some(native_order_index),
                })?;
                native_order_index += 1;
            } else {
                debug!(
                    "skipping {} P1448 native label because it has invalid type",
                    obj_id
                );
            }
        }
    }

    Ok(())
}

pub fn handle_line(
    mut line: &str,
    classes: &Classes,
    sink: &Sender<DataEntry>,
) -> Result<(), HandleLineError> {
    if line.len() <= 1 {
        // this is an empty line or one of the [ or ] array boundary lines
        return Ok(());
    }

    if line.ends_with(",") {
        line = &line[..line.len() - 1];
    }
    let obj: Value = serde_json::from_str(line)?;
    let obj_id = json_get!(value(obj).id: string).expect("object has no id!");

    if let Some(code_entries) = json_get!(value(obj).claims.P297: array) {
        let mut code_entry = None;
        for entry in code_entries {
            if is_object_active(json_get!(value(entry).qualifiers: object)) {
                code_entry = Some(entry);
                break;
            }
        }

        if let Some(iso) = json_get!(optval(code_entry).mainsnak.datavalue.value: string) {
            sink.send(DataEntry::Country {
                id: obj_id.into(),
                iso: iso.to_ascii_lowercase(),
            })?;
        }

        if let Some(langs) = json_get!(value(obj).claims.P37: array) {
            for lang in langs {
                if is_object_active(json_get!(value(lang).qualifiers: object)) {
                    continue;
                }
                if let Some(lang_id) = json_get!(value(lang).mainsnak.datavalue.value.id: string) {
                    sink.send(DataEntry::ObjectLanguage {
                        id: obj_id.into(),
                        lang_id: lang_id.into(),
                    })?;
                }
            }
        }
    }

    let is_territorial_entity = is_subclass_of(&obj, &classes.territorial_entities);
    let is_human_settlement = is_subclass_of(&obj, &classes.human_settlements);
    let is_lost_city = is_subclass_of(&obj, &classes.lost_cities);
    let is_language = is_subclass_of(&obj, &classes.languages);

    if is_territorial_entity && !is_lost_city {
        handle_territorial_entity(&obj, sink)?;
    }
    if is_human_settlement && !is_lost_city {
        handle_human_settlement(&obj, sink)?;
    }
    if is_language {
        handle_language(&obj, sink)?;
    }

    Ok(())
}

fn is_subclass_of(obj: &Value, classes: &HashSet<String>) -> bool {
    if let Some(parents) = json_get!(value(obj).claims.P31: array) {
        for parent in parents {
            if let Some(id) = json_get!(value(parent).mainsnak.datavalue.value.id: string) {
                if classes.contains(id) {
                    return true;
                }
            }
        }
    }
    false
}

#[derive(Debug, Error)]
pub enum HandleLineError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("crossbeam channel send error: {0}")]
    Sink(#[from] crossbeam::channel::SendError<DataEntry>),
}
