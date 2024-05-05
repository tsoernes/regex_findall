extern crate parquet;
extern crate time;
use parquet::file::reader::{FileReader, SerializedFileReader};

use parquet::record::Row;
use parquet::record::RowAccessor;
use parquet::schema::types::TypePtr;
use rayon::prelude::*;
use regex::escape;
use regex::Regex;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use time::PreciseTime;

fn main() -> std::io::Result<()> {
    let start_time = PreciseTime::now();

    let data_dir = Path::new("/tmp");
    let in_path = data_dir.join("job_desc.parquet");
    let out_path = data_dir.join("job_tags.json");
    let glassdoor_dir = Path::new("/home/torstein/code/fintechdb/Jobs/glassdoor");
    let tags_path = glassdoor_dir.join("tags.json");

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
    let id_to_tags = find_tags(&rows, regex, Some(&tag_set));
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
    println!("Read {} rows from {}", num_rows, in_path.display());

    let schema = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema_ptr();
    (rows, schema)
}

fn to_json<T: Sized + Serialize>(data: T, out_path: &Path) -> std::io::Result<()> {
    // Write output to JSON
    let json = serde_json::to_string(&data)?;
    let mut out_file = File::create(out_path)?;
    out_file.write_all(json.as_bytes())?;
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
                    let tags: Vec<String> = match tag_set {
                        Some(tag_set_) => regex
                            .find_iter(&desc)
                            .map(|m| m.as_str().to_owned())
                            .filter(|w| tag_set_.contains(w))
                            .collect(),
                        None => regex
                            .find_iter(&desc)
                            .map(|m| m.as_str().to_owned())
                            .collect(),
                    };

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
