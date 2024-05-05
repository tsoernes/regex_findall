extern crate parquet;
extern crate time;
use glob::glob;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs, sync::Arc};

use parquet::column::writer::ColumnWriter;
use parquet::record::Row;
use parquet::record::RowAccessor;
use parquet::{
    arrow::ArrowWriter,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
    schema::types::TypePtr,
};
use rayon::prelude::*;
use regex::escape;
use regex::Regex;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use time::PreciseTime;

fn get_paths(dir: &Path, glob_pattern: &str) -> Vec<PathBuf> {
    // Glob a directory and return a vector of paths.
    let glob_pattern = dir.join(glob_pattern);
    let globs = glob(glob_pattern.to_str().unwrap()).unwrap();
    let file_paths: Vec<PathBuf> = globs.filter_map(|p| p.ok()).collect();
    file_paths
}

fn main() -> std::io::Result<()> {
    let start_time = PreciseTime::now();

    let data_dir = Path::new("/tmp");
    let in_path = data_dir.join("job_desc.parquet");
    let out_path = data_dir.join("job_tags.json");
    let glassdoor_root = Path::new("/home/torstein/code/fintechdb/Jobs/glassdoor");
    let tags_path = glassdoor_root.join("tags.json");

    let n_rows = 10_000;

    // Construct regex for searching descriptions for tags
    let mut file = File::open(&tags_path).unwrap();
    let mut buff = String::new();
    file.read_to_string(&mut buff).unwrap();
    let tags: Vec<String> = serde_json::from_str(&buff).unwrap();
    // Wrap each tag with a word boundary and join them together with a `|`
    let re_tags: Vec<String> = tags
        .iter()
        .map(|tag| [r"\b", &escape(tag), r"\b"].concat())
        .collect();
    // Allow case insensitivity and join all tags together to a single regex
    let rex = [r"(?i)(", &re_tags.join("|"), ")"].concat();
    let regex = Regex::new(&rex).unwrap();

    let (rows, schema) = read_parquet(&in_path);
    println!("Schema {:?}", schema);
    let rows: Vec<Row> = rows.into_iter().take(n_rows).collect();

    let id_to_tags = find_tags(&rows, regex, None);
    to_json(id_to_tags, &out_path)?;

    let regex = Regex::new(r"(\w+)").unwrap();
    let tag_set: HashSet<String> = tags.into_iter().collect();
    let id_to_tags = find_tags(&rows, regex, &tag_set);
    to_json(id_to_tags, &out_path)?;

    let diff_time = start_time.to(PreciseTime::now());
    println!("Finished in {} seconds", diff_time);

    Ok(())
}

fn read_parquet(in_path: &Path) -> (Vec<Row>, TypePtr) {
    // Read Parquet input file with job description.
    // Returns a vector of rows, and the schema.
    let file = File::open(in_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let row_iter = reader.get_row_iter(None).unwrap();
    let num_rows = reader.metadata().file_metadata().num_rows();
    let rows: Vec<Row> = row_iter.collect();
    println!("num rows: {}", num_rows);
    let schema = reader.metadata().file_metadata().schema();

    // let writer = ArrowWriter::try_new(writer: W, arrow_schema: SchemaRef);

    let schema = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema_ptr();
    (rows, schema)
}

fn to_parquet(data: Vec<Row>, schema: TypePtr, out_path: &Path) {
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&out_path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        match col_writer {
            // You can also use `get_typed_column_writer` method to extract typed writer.
            ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                typed_writer.write_batch(&[1, 2, 3], None, None).unwrap();
            }
            _ => {}
        }
        row_group_writer.close_column(col_writer).unwrap();
    }
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}

fn to_json<T: Sized + Serialize>(data: T, out_path: &Path) -> std::io::Result<()> {
    // Write output to JSON
    let id_to_tags_json = serde_json::to_string(&data)?;
    let mut out_file = File::create(out_path)?;
    out_file.write_all(id_to_tags_json.as_bytes())?;
    Ok(())
}

fn find_tags(
    rows: &Vec<Row>,
    regex: Regex,
    tag_set: Option<&HashSet<String>>,
) -> HashMap<i64, Vec<String>> {
    // Find all tags matching the given `regex` in a list of job descriptions.
    // If a tag set is given, only tags contained in the given `tag_set` is retained.
    // Return a dictionary mapping each listing ID to a list of tags.

    // Dictionary mapping every listing ID to a list of tags
    let id_to_tags: HashMap<i64, Vec<String>> = rows
        .par_iter()
        .filter_map(|record| {
            // Filter out null strings
            match record.get_string(0) {
                Ok(desc) => {
                    // For each job description, search for tags

                    let desc = desc.replace(r"\\n", "");
                    let tags_iter = regex
                        .find_iter(&desc)
                        .map(|m| m.as_str().to_owned())
                    match tag_set {
                        Some(tag_set_) => {
                            let tags_iter = tags_iter.filter(|w| tag_set.contains(w))
                        },
                        _ => {}
                    }

                    let tags: Vec<String> = tags_iter.collect();

                    let id = record.get_long(1).unwrap();
                    Some((id, tags))
                }
                Err(_) => None,
            }
        })
        .collect();

    let total_tags: usize = id_to_tags.values().map(|vec| vec.len()).sum();
    println!("Total number of tags: {}", total_tags);

    id_to_tags
}
