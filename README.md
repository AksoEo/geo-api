# geo-db
Streams the entire WikiData database to extract a database of cities.

## Usage
### Building
To build a binary, install [Cargo](https://rust-lang.org) and run `cargo build --release` in this repository.
A binary will be available at `target/release/geo-db`.

### Running
Also see `./geo-db -h` for help.

To download the initial database of cities, run `./geo-db` with no arguments.
This will save it to a new database at `geo.db`.

Note that this database will require about 5 GB of space.
Downloading will use around 4 CPU cores and take 6–8 hours on a decent internet connection.

To run subsequent post-processing, run `./geo-db post`.
This will take around 30 minutes and may use up to 9 GB of space.
