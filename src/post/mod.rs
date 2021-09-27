use rusqlite::{params, Connection};
use std::time::Instant;

pub fn run(db_file: &str, do_post: bool, do_cleanup: bool) -> rusqlite::Result<()> {
    info!(
        "Opening database at {} (SQLite {})",
        db_file,
        rusqlite::version()
    );
    let conn = Connection::open(db_file)?;

    conn.execute_batch("PRAGMA cache_size = 100000;")?;

    if do_post {
        fn run_iter_labels(
            conn: &Connection,
            count_query: &str,
            iter_query: &str,
            per_row: &str,
        ) -> rusqlite::Result<()> {
            let unlabeled_city_count: u64 = conn.query_row(
                count_query,
                [],
                |row| row.get(0),
            )?;
            let status = {
                let (send, recv) = crossbeam::channel::unbounded();
                let status = std::thread::spawn(move || {
                    let start_time = Instant::now();
                    let mut last_time = Instant::now();
                    let mut rows_processed = 0;
                    let mut rows_processed_since_last = 0;
                    let mut item = String::from("?");
                    loop {
                        item = match recv.recv_timeout(std::time::Duration::from_secs(5)) {
                            Ok(item) => {
                                rows_processed += 1;
                                rows_processed_since_last += 1;
                                item
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Timeout) => item,
                            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                        };

                        if last_time.elapsed().as_secs() >= 10 {
                            if rows_processed_since_last == 0 {
                                error!("SQL got stuck on item {}", item);
                                std::process::exit(-1);
                            }

                            let progress = rows_processed as f64 / unlabeled_city_count as f64;

                            let rps = rows_processed_since_last as f64 / last_time.elapsed().as_secs_f64();

                            let elapsed_secs = start_time.elapsed().as_secs();
                            let secs = elapsed_secs % 60;
                            let mins = (elapsed_secs / 60) % 60;
                            let hours = elapsed_secs / 3600;

                            let time_elapsed = if hours > 0 {
                                format!("{}h {:02}m {:02}s", hours, mins, secs)
                            } else if mins > 0 {
                                format!("{:02}m {:02}s", mins, secs)
                            } else {
                                format!("{}s", secs)
                            };

                            let mut eta = (unlabeled_city_count - rows_processed) as f64 / rps / 60.;
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
                                "{:.2}% (ETA: {:.1}{}) | {}/{} rows in {} | {:.1} rows/s (at: {})",
                                progress * 100.,
                                eta,
                                eta_unit,
                                rows_processed,
                                unlabeled_city_count,
                                time_elapsed,
                                rps,
                                item,
                            );
                            last_time = Instant::now();
                            rows_processed_since_last = 0;
                        }
                    }
                    info!("Done!");
                });

                let mut row_stmt = conn.prepare(iter_query)?;
                let mut update_row = conn.prepare(per_row)?;

                let mut rows = row_stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let id = row.get_ref(0)?;
                    let id_str = id.as_str()?;
                    send.send(id_str.to_string())
                        .expect("failed to send status");

                    update_row.execute(params![id_str])?;
                }
                status
            };
            status.join().expect("status thread join failed");
            Ok(())
        }

        info!("Finding subdivisions");
        conn.execute_batch(include_str!("find_subdivision.sql"))?;

        info!("Updating city labels");
        conn.execute_batch(include_str!("city_labels.sql"))?;

        info!("Updating city labels recursively");
        run_iter_labels(
            &conn,
            "SELECT COUNT(1) as count FROM cities WHERE native_label IS NULL",
            "SELECT id FROM cities WHERE native_label IS NULL",
            include_str!("per_city.sql"),
        )?;

        info!("Updating city labels by country");
        conn.execute_batch(include_str!("city_labels_by_country.sql"))?;

        info!("Updating Esperanto city labels");
        conn.execute_batch(include_str!("esperanto_city_labels.sql"))?;

        info!("Updating subdivision labels");
        conn.execute_batch(include_str!("subdivision_labels.sql"))?;

        info!("Updating subdivision labels recursively");
        run_iter_labels(
            &conn,
            r#"SELECT count(DISTINCT "2nd_id") as count FROM cities WHERE "2nd_native_label" IS NULL AND "2nd_id" IS NOT NULL"#,
            r#"SELECT DISTINCT "2nd_id" FROM cities WHERE "2nd_native_label" IS NULL AND "2nd_id" IS NOT NULL"#,
            include_str!("per_subdivision.sql"),
        )?;

        info!("Updating subdivision labels by country");
        conn.execute_batch(include_str!("subdivision_labels_by_country.sql"))?;

        info!("Updating Esperanto subdivision labels");
        conn.execute_batch(include_str!("esperanto_subdivision_labels.sql"))?;
    }

    if do_cleanup {
        const SCRIPTS: &[(&str, &str)] = &[
            (include_str!("cleanup/01.sql"), "deleting territorial entities"),
            (include_str!("cleanup/02.sql"), "cleaning up object languages"),
            (include_str!("cleanup/03.sql"), "cleaning up object labels (may take a while)"),
            (include_str!("cleanup/04.sql"), "deleting unused tables"),
            (include_str!("cleanup/05.sql"), "cleaning up cities"),
            (include_str!("cleanup/06.sql"), "deleting unlabeled cities"),
            (include_str!("cleanup/07.sql"), "deleting unused object labels"),
            (include_str!("cleanup/08.sql"), "deleting unused object languages"),
            (include_str!("cleanup/09.sql"), "renaming tables"),
        ];

        for (i, (script, description)) in SCRIPTS.iter().enumerate() {
            info!("Clean-up step {}/{}: {}", i + 1, SCRIPTS.len(), description);
            conn.execute_batch(script)?;
        }
    }

    info!("Vacuuming database");
    conn.execute("VACUUM", [])?;

    info!("Done!");

    Ok(())
}
