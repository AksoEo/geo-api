use crate::database::DataEntry;
use crate::json_get;
use crate::wiki_sparql::Classes;
use crate::wiki_time::{is_object_active, parse_wikidata_time};
use crossbeam::channel::Sender;
use serde_json::Value;
use std::collections::HashSet;
use thiserror::Error;

/// both human settlements and territorial entities
fn handle_place(obj: &Value, sink: &Sender<DataEntry>) -> Result<(), HandleLineError> {
    let obj_id = json_get!(value(obj).id: string).unwrap();
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
                warn!(
                    "skipping TE {} P131 parent because it has no datavalue ID",
                    obj_id
                );
            }
        }
    }
    Ok(())
}

fn handle_territorial_entity(
    obj: &Value,
    is_2nd: bool,
    sink: &Sender<DataEntry>,
) -> Result<(), HandleLineError> {
    let obj_id = json_get!(value(obj).id: string).unwrap();
    sink.send(DataEntry::TerritorialEntity {
        id: obj_id.into(),
        is_2nd,
    })?;

    handle_place(obj, sink)?;

    // P37: official language
    // P2936: language used
    if let Some(langs) =
        json_get!(value(obj).claims.P37: array).or(json_get!(value(obj).claims.P2936: array))
    {
        let mut lang_index = 0;
        for lang in langs {
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
                    index: lang_index,
                })?;
                lang_index += 1;
            } else {
                warn!(
                    "skipping TE {} P37 lang because it has no datavalue ID",
                    obj_id
                );
            }
        }
    }

    if let Some(labels) = json_get!(value(obj).labels: object) {
        for label in labels.values() {
            if let (Some(lang), Some(label)) = (
                json_get!(value(label).language: string),
                json_get!(value(label).value: string),
            ) {
                sink.send(DataEntry::ObjectLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: None,
                })?;
            } else {
                warn!("skipping {} label because it has invalid type", obj_id);
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
        // warn!("skipping lang {} because it has no wikimedia language code", obj_id);
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

    handle_place(obj, sink)?;

    let mut country_id = None;
    for country_entry in country_entries {
        if is_object_active(json_get!(value(country_entry).qualifiers: object)) {
            if let Some(id) = json_get!(value(country_entry).mainsnak.datavalue.value.id: string) {
                country_id = Some(id.to_string());
            } else {
                warn!(
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
                        warn!(
                            "skipping {} P1082/P585 population entry because it has invalid time",
                            obj_id
                        );
                    }
                } else {
                    warn!(
                        "skipping {} P1082/P585 population entry because it has no time value",
                        obj_id
                    );
                }
            } else {
                // warn!("skipping {} P1082 population entry because it has no P585 entry", obj_id);
            }

            if let Some(_) = json_get!(value(population_entry).qualifiers.P518[0]: object) {
                // "applies to part" - but we want the entire population
                new_population_time = None; // reset to none
            }
            if let Some(_) = json_get!(value(population_entry).qualifiers.P1539[0]: object) {
                // this is only the female population
                new_population_time = None; // reset to none
            }
            if let Some(_) = json_get!(value(population_entry).qualifiers.P1540[0]: object) {
                // this is only the male population
                new_population_time = None; // reset to none
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
                        if unit != "1" {
                            // population is unitless!
                            continue;
                        }

                        if let Some(value) = parse_quantity(value) {
                            population = Some(value);
                            population_time = Some(new_time);
                        } else {
                            warn!("skipping {} P1082 population entry because its amount value could not be parsed as a number", obj_id);
                        }
                    } else {
                        warn!("skipping {} P1082 population entry because its amount value either does not exist or is an unexpected type", obj_id);
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
                warn!(
                    "skipping {} lat/lon because lat/lon are invalid types",
                    obj_id
                );
            }
        }
    } else {
        // warn!("skipping {} lat/lon because it has no P625 entry", obj_id);
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
                sink.send(DataEntry::ObjectLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: None,
                })?;
            } else {
                warn!("skipping {} label because it has invalid type", obj_id);
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
                sink.send(DataEntry::ObjectLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: Some(native_order_index),
                })?;
                native_order_index += 1;
            } else {
                warn!(
                    "skipping {} P1705 native label because it has invalid type",
                    obj_id
                );
            }
        }
    } else if let Some(official_names) = json_get!(value(obj).claims.P1448: array) {
        for claim in official_names {
            if !is_object_active(json_get!(value(claim).qualifiers: object)) {
                continue;
            }
            if let (Some(lang), Some(label)) = (
                json_get!(value(claim).mainsnak.datavalue.value.language: string),
                json_get!(value(claim).mainsnak.datavalue.value.text: string),
            ) {
                sink.send(DataEntry::ObjectLabel {
                    id: obj_id.into(),
                    lang: lang.into(),
                    label: label.into(),
                    native_order: Some(native_order_index),
                })?;
                native_order_index += 1;
            } else {
                warn!(
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

    if json_get!(value(obj).claims.P1366: array).map_or(false, |a| !a.is_empty())
        || json_get!(value(obj).claims.P576: array).map_or(false, |a| !a.is_empty())
    {
        // P1366: "replaced by"
        // P576: "dissolved date"
        // -> don't care about this object
        return Ok(());
    }

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

        let mut lang_index = 0;
        if let Some(langs) = json_get!(value(obj).claims.P37: array) {
            for lang in langs {
                if !is_object_active(json_get!(value(lang).qualifiers: object)) {
                    continue;
                }
                if let Some(lang_id) = json_get!(value(lang).mainsnak.datavalue.value.id: string) {
                    sink.send(DataEntry::ObjectLanguage {
                        id: obj_id.into(),
                        lang_id: lang_id.into(),
                        index: lang_index,
                    })?;
                    lang_index += 1;
                }
            }
        }
    }

    let is_territorial_entity = is_subclass_of(&obj, &classes.territorial_entities);
    let is_human_settlement = is_subclass_of(&obj, &classes.human_settlements);
    let is_excluded = is_subclass_of(&obj, &classes.excluded);
    let is_language = is_subclass_of(&obj, &classes.languages);

    if is_territorial_entity && !is_excluded {
        let is_2nd = is_subclass_of(&obj, &classes.second_level_admin_div);
        handle_territorial_entity(&obj, is_2nd, sink)?;
    }
    if is_human_settlement && !is_excluded {
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

fn parse_quantity(n: &str) -> Option<u64> {
    let should_keep_char = |c: &char| match c {
        c if c.is_whitespace() => false,
        ',' | '.' | '+' => false, // thousands separators
        _ => true,
    };

    if n.contains(|c| !should_keep_char(&c)) {
        n.chars()
            .filter(should_keep_char)
            .collect::<String>()
            .parse()
            .ok()
    } else {
        n.parse().ok()
    }
}
