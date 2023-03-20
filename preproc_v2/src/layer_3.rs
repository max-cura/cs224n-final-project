use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use chrono::NaiveDateTime;
use clap::Args;
use indicatif::ProgressIterator;
use quick_xml::events::Event;
use quick_xml::Reader;

#[derive(Args)]
pub struct Layer3Args {
    #[arg(long = "in-file", required=true)]
    infile: PathBuf,
    #[arg(long = "in-layer-2", required=true)]
    layer2: PathBuf,
    #[arg(long = "out-file", required=true)]
    outfile: PathBuf,
    #[arg(long = "l2-count", default_value_t=2431869)]
    layer2_size: u64,
    #[arg(long = "vcount", default_value_t=449071008)]
    v_count: u64,
    #[arg(long = "flush-interval", default_value_t=1_000_000)]
    flush_interval: usize,
}

struct VCounter {
    edit_time: NaiveDateTime,
    up_before: u32,
    down_before: u32,
    up_after: u32,
    down_after: u32,
}

fn layer3_load_l2_indices(args: &Layer3Args) -> BTreeMap<i64, VCounter> {
    println!("Loading question index from Layer2 at {}", args.layer2.display());
    let pb = crate::progress_bar(args.layer2_size);
    let reader = BufReader::new(OpenOptions::new()
        .read(true)
        .open(&args.layer2)
        .expect("Failed to open IN_LAYER2"));
    let dataset = reader.lines()
        .progress_with(pb.clone())
        .map(|line| {
            let line = line.unwrap();
            let post_id = {
                let first_tab = line.find('\t').expect("No tab in line in TSV");
                i64::from_str(&line[0..first_tab]).unwrap()
            };
            let edit_time = {
                let last_tab = line.rfind('\t').expect("No (last) tab in line in TSV");
                NaiveDateTime::parse_from_str(&line[(last_tab+1)..], crate::DATE_FORMAT).unwrap()
            };
            (post_id, VCounter {
                edit_time,
                up_before: 0,
                down_before: 0,
                up_after: 0,
                down_after: 0,
            })
        })
        .collect::<BTreeMap<i64, VCounter>>();
    pb.finish();
    println!("Loaded {} items from Layer2 results", dataset.len());
    dataset
}

fn layer3_tabulate_vote_counts(args: &Layer3Args, vote_map: &mut BTreeMap<i64, VCounter>) {
    let mut n_votes = 0;
    let mut n_proc = 0;

    let pb = crate::progress_bar(args.v_count);
    let mut reader = Reader::from_file(&args.infile)
        .expect("Failed to open INFILE for reading");
    let mut xml_buf = Vec::new();

    println!("Tabulating relevant vote counts from {}", args.infile.display());

    loop {
        pb.inc(1);
        n_proc += 1;

        match reader.read_event_into(&mut xml_buf) {
            Err(e) => panic!("Error at position {}: {e}", reader.buffer_position()),
            Ok(Event::Eof) => break,
            Ok(Event::Empty(element)) => {
                if element.name().as_ref() == b"row" {
                    let mut attrs = element.attributes();
                    // ignore row's Id
                    let _ = attrs.next();
                    let post_id_attr = attrs.next().unwrap().unwrap();
                    assert_eq!(post_id_attr.key.as_ref(), b"PostId");
                    let post_id = i64::from_str(
                        std::str::from_utf8(post_id_attr.value.as_ref()).unwrap()
                    ).unwrap();
                    let mapped = vote_map.get_mut(&post_id);
                    if let Some(vcounter) = mapped {
                        let vote_type_attr = attrs.next().unwrap().unwrap();
                        assert_eq!(vote_type_attr.key.as_ref(), b"VoteTypeId");
                        let vote_type = vote_type_attr.value.as_ref();
                        if vote_type != b"2" && vote_type != b"3" {
                            continue;
                        }
                        let date_attr = attrs.next().unwrap().unwrap();
                        assert_eq!(date_attr.key.as_ref(), b"CreationDate");
                        let date = NaiveDateTime::parse_from_str(
                            std::str::from_utf8(date_attr.value.as_ref()).unwrap(),
                            crate::DATE_FORMAT
                        ).unwrap();
                        // 2 is up, 3 is down
                        if vcounter.edit_time.date() < date.date() {
                            if vote_type == b"2" {
                                vcounter.up_before += 1;
                            } else {
                                vcounter.down_before += 1;
                            }
                        } else if vcounter.edit_time.date() > date.date() {
                            if vote_type == b"2" {
                                vcounter.up_after += 1;
                            } else {
                                vcounter.down_after += 1;
                            }
                        }
                        n_votes += 1;
                    }
                }
            }
            _ => (),
        }

        xml_buf.clear();
    }

    pb.finish();
    println!("Tabulated {n_votes}/{n_proc} votes!");
}

fn layer3_write(args: &Layer3Args, vote_map: &BTreeMap<i64, VCounter>) {
    let pb = crate::progress_bar(vote_map.len() as u64);

    println!("Writing vote counts for {} questions.", vote_map.len());

    let mut writer = BufWriter::new(OpenOptions::new()
        .write(true).create(true).truncate(true).open(&args.outfile)
        .expect("Couldn't open OUTFILE for writing"));

    for (post_id, vcounts) in vote_map {
        pb.inc(1);

        writeln!(writer, "{}\t{}\t{}\t{}\t{}",
            post_id,
            vcounts.up_before,
            vcounts.down_before,
            vcounts.up_after,
            vcounts.down_after
        ).unwrap();
        if writer.buffer().len() >= args.flush_interval {
            writer.flush().unwrap();
        }
    }

    writer.flush().unwrap();
    pb.finish();

    println!("Finished writing.");
}

pub fn layer3_filter(args: &Layer3Args) {
    let mut vote_map = layer3_load_l2_indices(args);

    layer3_tabulate_vote_counts(args, &mut vote_map);

    layer3_write(args, &vote_map)
}