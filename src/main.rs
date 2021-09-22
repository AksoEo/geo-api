#[macro_use]
extern crate log;

use crate::input::DataInput;
use clap::{App, Arg, SubCommand};
use std::process::exit;
use std::sync::Arc;

mod database;
mod input;
mod json;
mod post;
mod wiki_data_line;
mod wiki_sparql;
mod wiki_time;

fn main() {
    let matches = App::new("geo-db")
        .about("streams the latest WikiData dump and saves it to a file")
        .arg(
            Arg::with_name("out")
                .short("o")
                .long("output")
                .help("Sets the output file")
                .takes_value(true)
                .default_value("geo.db"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Prints debug info"),
        )
        .subcommand(
            SubCommand::with_name("entity")
                .about("loads a single entity and prints generated database entries")
                .arg(
                    Arg::with_name("entity")
                        .help("the entity id(s) (including Q)")
                        .index(1)
                        .takes_value(true)
                        .multiple(true)
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("post")
                .about("performs post-processing on the database")
                .arg(
                    Arg::with_name("database")
                        .help("the database file")
                        .index(1)
                        .takes_value(true)
                        .default_value("geo.db"),
                )
                .arg(
                    Arg::with_name("only_cleanup")
                        .help("only performs the cleanup step")
                        .long("only-cleanup")
                )
                .arg(
                    Arg::with_name("skip_cleanup")
                        .help("skips the cleanup step")
                        .long("no-cleanup")
                ),
        )
        .get_matches();

    let colors = fern::colors::ColoredLevelConfig::new();
    fern::Dispatch::new()
        .format(move |out, msg, record| {
            out.finish(format_args!(
                "{}\x1b[{}m[{} {}] {}\x1b[m",
                chrono::Local::now().format("[%H:%M:%S]"),
                colors.get_color(&record.level()).to_fg_str(),
                record.level(),
                record.target(),
                msg
            ))
        })
        .level(if matches.is_present("verbose") {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        })
        .chain(std::io::stdout())
        .apply()
        .unwrap();

    match matches.subcommand() {
        ("entity", Some(args)) => {
            let ids = args.values_of("entity").expect("no entity id");
            match debug_entities(ids) {
                Ok(()) => {}
                Err(e) => error!("{}", e),
            }
        }
        ("post", Some(args)) => {
            let db_file = args.value_of("database").expect("no database file");
            let only_cleanup = args.is_present("only_cleanup");
            let skip_cleanup = args.is_present("skip_cleanup");
            let (do_post, do_cleanup) = match (only_cleanup, skip_cleanup) {
                (true, true) => {
                    error!("Can’t both do cleanup and not do cleanup");
                    exit(-1);
                }
                (true, false) => (false, true),
                (false, true) => (true, false),
                (false, false) => (true, true),
            };
            match post::run(db_file, do_post, do_cleanup) {
                Ok(()) => {}
                Err(e) => error!("{}", e),
            }
        }
        _ => {
            let out_file = matches.value_of("out").expect("no output file");
            run(out_file.into());
        }
    }
}

fn run(out_file: String) {
    let url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2";
    let db_writer = {
        let data_input = input::http::HttpBz2DataInput::new(url.into());
        // let data_input = input::file::Bz2FileInput::new(std::fs::File::open(file).unwrap());
        let mut lines = input::InputLineIter::new(data_input);

        info!("Loading classes");
        let classes = Arc::new(match wiki_sparql::Classes::new_from_http() {
            Ok(classes) => classes,
            Err(e) => {
                error!("Failed to fetch classes: {}", e);
                exit(-1);
            }
        });

        info!("Streaming data from {} to {}", url, out_file);

        let (send, recv) = crossbeam::channel::unbounded();

        let db_writer = std::thread::spawn(move || match database::db_writer(&out_file, recv) {
            Ok(()) => (),
            Err(e) => {
                error!("database writer exited with error: {}", e);
                exit(-1);
            }
        });

        let (cancel_send, cancel_recv) = crossbeam::channel::bounded(3);
        ctrlc::set_handler(move || cancel_send.send(()).unwrap())
            .expect("could not set interrupt handler");

        let mut last_time = std::time::Instant::now();
        let mut last_bytes = 0;
        let mut last_dec_bytes = 0;
        let mut line_number = 0;
        loop {
            match cancel_recv.try_recv() {
                Ok(()) => {
                    debug!("received interrupt signal");
                    break;
                }
                Err(crossbeam::channel::TryRecvError::Empty) => (),
                Err(e) => panic!("unexpected error {}", e),
            }

            let line_offset = lines.bytes_read;
            line_number += 1;
            let line = match lines.next() {
                Ok(line) => line,
                Err(input::LineIterError::Eof) => break,
                Err(e) => {
                    error!("line iterator error: {}", e);
                    exit(-1);
                }
            };

            let sink = send.clone();
            let classes2 = Arc::clone(&classes);
            rayon_core::spawn(
                move || match wiki_data_line::handle_line(&line, &classes2, &sink) {
                    Ok(()) => (),
                    Err(e) => error!(
                        "error handling line {} at offset {}:{}\n\n",
                        line_number, line_offset, e
                    ),
                },
            );

            let elapsed = last_time.elapsed();
            if elapsed.as_secs() > 10 {
                let bytes_read =
                    (lines.input.bytes_read() - last_bytes) as f64 / elapsed.as_secs_f64();
                let dec_bytes_read =
                    (lines.bytes_read - last_dec_bytes) as f64 / elapsed.as_secs_f64();
                let total_bytes = lines.input.content_length().unwrap_or(0);
                let percent_complete = lines.input.bytes_read() as f64 / total_bytes as f64;
                let mut eta = (total_bytes - lines.input.bytes_read()) as f64 / bytes_read / 60.;
                let mut eta_unit = "m";
                if eta > 60. {
                    eta /= 60.;
                    eta_unit = "h";

                    if eta > 24. {
                        eta /= 24.;
                        eta_unit = "d 😔";
                    }
                }

                info!(
                    "{:02.2}% (ETA: {:.1}{}) | {:.2} MB of {:.2} MB at {:.2} MB/s ({:.2} MB/s data)",
                    percent_complete * 100.,
                    eta,
                    eta_unit,
                    lines.input.bytes_read() as f64 / 1000_000.,
                    total_bytes as f64 / 1000_000.,
                    bytes_read / 1000_000.,
                    dec_bytes_read / 1000_000.,
                );
                last_bytes = lines.input.bytes_read();
                last_dec_bytes = lines.bytes_read;
                last_time = std::time::Instant::now();
            }
        }

        db_writer
    };

    debug!("Waiting for DB writer to join");
    db_writer.join().unwrap();
    info!("Done!");
}

fn debug_entities<'a>(ids: impl Iterator<Item = &'a str>) -> reqwest::Result<()> {
    info!("Loading classes");
    let classes = wiki_sparql::Classes::new_from_http()?;

    for id in ids {
        let url = format!("https://wikidata.org/wiki/Special:EntityData/{}.json", id);
        let json: serde_json::Value = match reqwest::blocking::get(url).and_then(|res| res.json()) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to fetch entity {}: {}", id, e);
                continue;
            }
        };
        if let Some(entity) = json
            .as_object()
            .and_then(|root| root.get("entities"))
            .and_then(|entities| entities.as_object())
            .and_then(|entities| entities.get(id))
            .and_then(|entity| serde_json::to_string(entity).ok())
        {
            info!("Entity {}", id);

            let (send, recv) = crossbeam::channel::unbounded();
            match wiki_data_line::handle_line(&entity, &classes, &send) {
                Ok(()) => {}
                Err(e) => {
                    error!("{}", e);
                }
            }

            while let Ok(entry) = recv.try_recv() {
                if let database::DataEntry::ObjectLabel { .. } = &entry {
                    info!("{}: {:?}", id, entry);
                } else {
                    info!("{}: {:#?}", id, entry);
                }
            }
        } else {
            error!("Entity {}: invalid data", id);
        }
    }

    info!("Done!");
    Ok(())
}
