use bincode;
use blitzdb_common::IndexEntry;
use boomphf::Mphf;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn prepare(input: &Path, prefix: &Path) -> anyhow::Result<()> {
    // Read Parquet
    let file = File::open(input)?;
    let reader = SerializedFileReader::new(file)?;
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let row_iter = reader.get_row_iter(None)?;
    for row in row_iter {
        let row = row?;
        let key = row.get_bytes(0)?.data().to_vec();
        let val = row.get_bytes(1)?.data().to_vec();
        pairs.push((key, val));
    }
    let n = pairs.len();

    // Build MPH
    let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
    let mph = Mphf::<Vec<u8>>::new_parallel(1.02, &keys, None);

    // Build index and heap
    let mut index: Vec<IndexEntry> = vec![IndexEntry::default(); n];
    let mut heap: Vec<u8> = Vec::new();
    for (key, val) in &pairs {
        let slot = mph.hash(key) as usize;
        let offset = heap.len() as u64;
        index[slot] = IndexEntry::new(key, offset, val.len() as u32);
        heap.extend_from_slice(val);
    }

    // Write .mph
    let mph_path = prefix.with_extension("mph");
    bincode::serialize_into(File::create(mph_path)?, &mph)?;

    // Write .index
    let index_path = prefix.with_extension("index");
    let mut index_writer = BufWriter::new(File::create(index_path)?);
    for entry in &index {
        entry.write_to(&mut index_writer)?;
    }
    index_writer.flush()?;

    // Write .heap
    let heap_path = prefix.with_extension("heap");
    let mut heap_writer = BufWriter::new(File::create(heap_path)?);
    heap_writer.write_all(&heap)?;
    heap_writer.flush()?;

    Ok(())
}
