use crate::input::http::USER_AGENT;
use reqwest::header;
use reqwest::Url;
use serde::Deserialize;
use std::collections::HashSet;

const BASE_URL: &str = "https://query.wikidata.org/sparql";

pub fn load_subclasses(parent_class: &str) -> reqwest::Result<HashSet<String>> {
    debug!("Loading subclasses for {:?}", parent_class);
    let mut url = Url::parse(BASE_URL).expect("bad BASE_URL!");
    url.query_pairs_mut().append_pair(
        "query",
        &format!("SELECT ?s WHERE {{ ?s wdt:P279+ wd:{} . }}", parent_class),
    );

    #[derive(Deserialize)]
    struct SparqlResult {
        results: SparqlInnerResults,
    }
    #[derive(Deserialize)]
    struct SparqlInnerResults {
        bindings: Vec<SparqlEntity>,
    }
    #[derive(Deserialize)]
    struct SparqlEntity {
        s: SparqlEntityS,
    }
    #[derive(Deserialize)]
    struct SparqlEntityS {
        value: String,
    }

    let result: SparqlResult = reqwest::blocking::Client::builder()
        .user_agent(USER_AGENT)
        .build()?
        .get(url)
        .header(
            header::ACCEPT,
            "application/sparql-results+json;charset=utf-8",
        )
        .send()?
        .json()?;

    let classes: HashSet<String> = result
        .results
        .bindings
        .into_iter()
        .filter_map(|entity| {
            Some(
                Url::parse(&entity.s.value)
                    .ok()?
                    .path_segments()?
                    .last()?
                    .to_string(),
            )
        })
        .collect();

    debug!(
        "Successfully loaded {} subclasses for parent class {:?}",
        classes.len(),
        parent_class
    );

    Ok(classes)
}

pub struct Classes {
    pub territorial_entities: HashSet<String>,
    pub human_settlements: HashSet<String>,
    pub lost_cities: HashSet<String>,
    pub neighborhoods: HashSet<String>,
    pub second_level_admin_div: HashSet<String>,
    pub languages: HashSet<String>,
}

impl Classes {
    pub fn new_from_http() -> reqwest::Result<Classes> {
        let mut territorial_entities = load_subclasses("Q56061")?;
        territorial_entities.insert("Q56061".into());

        let mut human_settlements = load_subclasses("Q486972")?;
        human_settlements.insert("Q486972".into());

        let mut lost_cities = load_subclasses("Q2974842")?;
        lost_cities.insert("Q2974842".into());

        let mut lost_cities = load_subclasses("Q2974842")?;
        lost_cities.insert("Q2974842".into());

        let mut neighborhoods = load_subclasses("Q123705")?;
        neighborhoods.insert("Q123705".into());

        let mut second_level_admin_div = load_subclasses("Q13220204")?;
        second_level_admin_div.insert("Q13220204".into());

        let mut languages = HashSet::new();
        languages.insert("Q34770".into());

        Ok(Classes {
            human_settlements,
            territorial_entities,
            lost_cities,
            neighborhoods,
            second_level_admin_div,
            languages,
        })
    }
}
