use boomphf::Mphf;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

fn main() {
    let mut args = std::env::args().skip(1);
    let input = args.next().expect("Usage: blitzdb-prepare <input.parquet> [output-prefix]");
    let prefix = args.next().unwrap_or_else(|| {
        Path::new(&input)
            .with_extension("")
            .to_string_lossy()
            .into_owned()
    });

    // --- Read Parquet ---
    let file = File::open(&input).expect("Failed to open parquet file");
    let reader = SerializedFileReader::new(file).expect("Failed to create parquet reader");

    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let row_iter = reader.get_row_iter(None).expect("Failed to get row iterator");
    for row in row_iter {
        let row = row.expect("Failed to read row");
        let key = row.get_bytes(0).expect("Missing 'key' column").data().to_vec();
        let val = row.get_bytes(1).expect("Missing 'value' column").data().to_vec();
        pairs.push((key, val));
    }
    let n = pairs.len();
    println!("Read {n} rows from {input}");

    // --- Build MPH ---
    let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
    let mph = Mphf::<Vec<u8>>::new_parallel(2.0, &keys, None);
    println!("Built minimal perfect hash over {n} keys");

    // --- Build index and heap ---
    // index[slot] = (heap_offset: u64, value_len: u32)
    let mut index: Vec<(u64, u32)> = vec![(0, 0); n];
    let mut heap: Vec<u8> = Vec::new();
    for (key, val) in &pairs {
        let slot = mph.hash(key) as usize;
        let offset = heap.len() as u64;
        index[slot] = (offset, val.len() as u32);
        heap.extend_from_slice(val);
    }

    // --- Write .mph ---
    let mph_path = PathBuf::from(format!("{prefix}.mph"));
    let mph_file = File::create(&mph_path).expect("Failed to create .mph file");
    bincode::serialize_into(mph_file, &mph).expect("Failed to serialize MPH");
    println!("Wrote {}", mph_path.display());

    // --- Write .index ---
    let index_path = PathBuf::from(format!("{prefix}.index"));
    let mut index_writer = BufWriter::new(File::create(&index_path).expect("Failed to create .index file"));
    for (offset, len) in &index {
        index_writer.write_all(&offset.to_le_bytes()).unwrap();
        index_writer.write_all(&len.to_le_bytes()).unwrap();
    }
    index_writer.flush().unwrap();
    let index_bytes = n * 12;
    println!("Wrote {} ({index_bytes} bytes, {n} entries)", index_path.display());

    // --- Write .heap ---
    let heap_path = PathBuf::from(format!("{prefix}.heap"));
    let mut heap_writer = BufWriter::new(File::create(&heap_path).expect("Failed to create .heap file"));
    heap_writer.write_all(&heap).unwrap();
    heap_writer.flush().unwrap();
    println!("Wrote {} ({} bytes)", heap_path.display(), heap.len());
}
