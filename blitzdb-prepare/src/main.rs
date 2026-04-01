use std::path::Path;

fn main() {
    let mut args = std::env::args().skip(1);
    let input = args
        .next()
        .expect("Usage: blitzdb-prepare <input.parquet> [output-prefix]");
    let prefix = args.next().unwrap_or_else(|| {
        Path::new(&input)
            .with_extension("")
            .to_string_lossy()
            .into_owned()
    });
    blitzdb_prepare::prepare(Path::new(&input), Path::new(&prefix))
        .expect("blitzdb-prepare failed");
}
