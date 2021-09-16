use rusqlite::{params, Connection};
use std::time::Instant;

pub fn run(db_file: &str) -> rusqlite::Result<()> {
    info!(
        "Opening database at {} (SQLite {})",
        db_file,
        rusqlite::version()
    );
    let conn = Connection::open(db_file)?;

    conn.execute_batch("PRAGMA cache_size = 100000;")?;

    info!("Updating city labels");
    conn.execute_batch(include_str!("city_labels.sql"))?;

    info!("Updating city labels recursively");
    let unlabeled_city_count: u64 = conn.query_row(
        "SELECT COUNT(1) as count FROM cities WHERE native_label IS NULL",
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

        let mut cities = conn.prepare("SELECT id FROM cities WHERE native_label IS NULL")?;
        let mut update_city = conn.prepare(include_str!("per_city.sql"))?;

        let mut rows = cities.query([])?;
        while let Some(row) = rows.next()? {
            let id = row.get_ref(0)?;
            let id_str = id.as_str()?;
            send.send(id_str.to_string())
                .expect("failed to send status");

            update_city.execute(params![id_str])?;
        }
        status
    };

    status.join().expect("status thread join failed");
    info!("Done!");

    Ok(())
}
