use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use chrono::NaiveDateTime;
use clap::Args;
use indicatif::ProgressIterator;

#[derive(Args)]
pub struct Layer4Args {
    #[clap(long="in-layer-2", required=true)]
    layer2: PathBuf,
    #[clap(long="in-layer-3", required=true)]
    layer3: PathBuf,
    #[clap(long="out-base", required=true)]
    out_base: PathBuf,
    #[clap(long="l2-count", default_value_t=2431869)]
    l2_count: u64,
    #[clap(long="split")]
    split: String,
    #[clap(long="flush-interval", default_value_t=1_000_000)]
    flush_interval: usize,
}

struct XInfo {
    created_date: NaiveDateTime,
    edit_date: NaiveDateTime,
    pre_edit_up_votes: u32,
    post_edit_up_votes: u32,
    pre_edit_down_votes: u32,
    post_edit_down_votes: u32,
}

fn layer4_simple_filters(args: &Layer4Args) -> BTreeMap<i64, XInfo> {
    let reader = BufReader::new(OpenOptions::new()
        .read(true).open(&args.layer2)
        .expect("Failed to open IN_LAYER_3 for reading"));
    let pb = crate::progress_bar(args.l2_count);

    fn scan_for_code(s: &str) -> bool {
        if s.contains("&#xD;&#xA;    ") {
            return true;
        }
        let backtick_count = s.matches('`').count();
        let escaped_backtick_count = s.matches("\\`").count();
        if backtick_count > escaped_backtick_count {
            return true;
        }
        false
    }

    fn estimate_token_count(s: &str) -> usize {
        let ws_delimited : Vec<_> = s.split_whitespace().collect();
        let mut n_tokens = 1;

        for x in ws_delimited {
            let mut prev = '\n';
            for c in x.chars() {
                if c.is_ascii_punctuation() {
                    n_tokens += 1;
                }
                if c.is_alphabetic() && (prev != '\n' && !prev.is_alphabetic()) {
                    n_tokens += 1;
                }
                if c.is_numeric() && (prev != '\n' && !prev.is_numeric()) {
                    n_tokens += 1;
                }
                prev = c;
            }
        }

        n_tokens
    }

    // let mut deny_length = 0;
    // let mut deny_code = 0;

    println!("Running deny filters in {} Layer2 inputs...", args.l2_count);

    let dataset = reader.lines()
        .progress_with(pb.clone())
        .filter_map(|line| {
            let line = line.unwrap();
            if estimate_token_count(&line) > 200 {
                // deny_length += 1;
                return None
            }
            if scan_for_code(&line) {
                // deny_code += 1;
                return None
            }
            let items = line.split('\t').collect::<Vec<_>>();
            assert_eq!(items.len(), 5);
            let created_date = NaiveDateTime::parse_from_str(items[2], crate::DATE_FORMAT)
                .map_err(|e| {
                    println!("{items:?}");
                    e
                })
                .unwrap();
            let edit_date = NaiveDateTime::parse_from_str(items[4], crate::DATE_FORMAT)
                .map_err(|e| {
                    println!("{items:?}");
                    e
                })
                .unwrap();
            Some((
                i64::from_str(items[0]).unwrap(),
                XInfo {
                    created_date,
                    edit_date,
                    pre_edit_up_votes: 0,
                    post_edit_up_votes: 0,
                    pre_edit_down_votes: 0,
                    post_edit_down_votes: 0,
                }
            ))
        })
        .collect::<BTreeMap<i64, XInfo>>();

    pb.finish();
    println!("Deny filters yield {} examples", dataset.len());

    dataset
}

fn layer4_generate(args: &Layer4Args, posts: &BTreeSet<i64>) {
    let split = args.split.split(':').map(|s| i64::from_str(s).unwrap()).collect::<Vec<_>>();
    let train_count = split[0];
    let eval_count = split[1];
    let test_count = split[2];
    let total = train_count + eval_count + test_count;

    let train_prop = (train_count as f32) / (total as f32);
    let eval_prop = (eval_count as f32) / (total as f32);
    let test_prop = (test_count as f32) / (total as f32);

    let actual_total = posts.len() as f32;
    // let actual_train_count = (actual_total * train_prop).round() as usize;
    let actual_eval_count = (actual_total * eval_prop).round() as usize;
    let actual_test_count = (actual_total * test_prop).round() as usize;
    // assert_eq!(actual_train_count + actual_eval_count + actual_test_count, posts.len());
    let actual_train_count = posts.len() - actual_eval_count - actual_test_count;
    println!("actual_total={actual_total}, actual_train_count={actual_train_count}, actual_eval_count={actual_eval_count}, actual_test_count={actual_test_count}");

    let fname_base = args.out_base.file_stem().unwrap();
    fn append(a: &OsStr, b: &OsStr) -> OsString {
        let mut x = a.to_os_string();
        x.push(b);
        x
    }
    let train_path = args.out_base.with_file_name(append(fname_base, OsStr::new("-train.tsv")));
    let eval_path = args.out_base.with_file_name(append(fname_base, OsStr::new("-eval.tsv")));
    let test_path = args.out_base.with_file_name(append(fname_base, OsStr::new("-test.tsv")));

    let mut reader = BufReader::new(OpenOptions::new()
        .read(true).open(&args.layer2)
        .expect("Failed to open IN_LAYER_2 for reading"));

    layer4_write(&train_path, actual_train_count, &mut reader, posts, args.flush_interval);
    layer4_write(&eval_path, actual_eval_count, &mut reader, posts, args.flush_interval);
    layer4_write(&test_path, actual_test_count, &mut reader, posts, args.flush_interval);
}

fn layer4_write(file: &Path, count: usize, reader: &mut BufReader<File>, posts: &BTreeSet<i64>, flush_interval: usize) {
    let mut written = 0;
    let mut buf = String::new();

    let mut writer = BufWriter::new(OpenOptions::new()
        .write(true).truncate(true).create(true).open(file)
        .expect(format!("Failed to open OUT_TRAIN_PATH ({}) for writing", file.display()).as_str()));

    let pb = crate::progress_bar(count as u64);

    println!("Writing {count} examples to {}", file.display());

    writeln!(writer, "input\toutput").unwrap();

    while written < count {
        reader.read_line(&mut buf).unwrap();

        let line_split = buf.split('\t').collect::<Vec<_>>();
        let post_id = i64::from_str(line_split[0]).unwrap();

        if posts.contains(&post_id) {
            pb.inc(1);
            written += 1;
            writeln!(writer, "{}\t{}", line_split[1], line_split[3]).unwrap();
            if writer.buffer().len() > flush_interval {
                writer.flush().unwrap();
            }
        }

        buf.clear();
    }

    writer.flush().unwrap();
    pb.finish();
    println!("Finished!");
}

pub fn layer4_filter(args: &Layer4Args) {
    let simple_filtered = layer4_simple_filters(args);

    let posts = simple_filtered.keys().map(|x| *x).collect::<BTreeSet<i64>>();

    layer4_generate(&args, &posts);
}