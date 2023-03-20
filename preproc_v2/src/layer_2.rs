use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str::FromStr;
use chrono::NaiveDateTime;
use clap::Args;
use indicatif::ProgressIterator;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

#[derive(Args)]
pub struct Layer2Args {
    #[arg(long = "in-file", required=true)]
    infile: PathBuf,
    #[arg(long = "in-layer-1", required=true)]
    layer1: PathBuf,
    #[arg(long = "out-file", required=true)]
    outfile: PathBuf,
    #[arg(long = "l1-count", default_value_t=7064714)]
    layer1_size: u64,
    #[arg(long = "hcount", default_value_t=234510258)]
    h_count: u64,
    #[arg(long = "flush-interval", default_value_t=1_000_000)]
    flush_interval: usize,
}

struct QInfo {
    delete: bool,
    author_id: i32,
    before_position: u64,
    after_position: u64,
}

fn load_layer_1(args: &Layer2Args) -> BTreeMap<i64, QInfo> {
    // format is {post_id}\t{author_id}
    println!("Loading question index from {}", args.layer1.display());
    let pb = crate::progress_bar(args.layer1_size);
    let f = OpenOptions::new()
        .read(true)
        .open(&args.layer1)
        .expect("Failed to open layer1 data file for reading");
    let reader = BufReader::new(f);
    let dataset = reader.lines()
        .progress_with(pb.clone())
        .map(|line| {
            let line = line.unwrap();
            let splits: Vec<_> = line.split('\t').collect();
            assert_eq!(splits.len(), 2);
            (
                i64::from_str(splits[0]).unwrap(),
                QInfo {
                    author_id: i32::from_str(splits[1]).unwrap(),
                    delete: false,
                    before_position: u64::MAX,
                    after_position: u64::MAX,
                }
            )
        }).collect::<BTreeMap<i64, QInfo>>();
    pb.finish();
    println!("Loaded {} question items from Layer1.", dataset.len());
    dataset
}

enum Layer2ScanFilterAction {
    Ignore,
    Delete(i64),
    AddBefore(i64),
    AddAfter(i64),
}

fn layer2_scan(args: &Layer2Args, l1: &mut BTreeMap<i64, QInfo>) -> u64 {
    let mut total_items = l1.len() as u64;

    let mut reader = Reader::from_file(&args.infile)
        .expect("Failed to open INFILE for reading");
    let mut xml_buf = Vec::new();
    let pb = crate::progress_bar(args.h_count);

    println!("Loading question histories from {}", args.infile.display());

    loop {
        pb.inc(1);

        let pre_buf_pos = reader.buffer_position();

        match reader.read_event_into(&mut xml_buf) {
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            Ok(Event::Eof) => break,
            Ok(Event::Empty(element)) => {
                if element.name().as_ref() == b"row" {
                    match layer2_scan_filter(element, l1) {
                        Layer2ScanFilterAction::Ignore => {}
                        Layer2ScanFilterAction::Delete(y) => {
                            l1.get_mut(&y).unwrap().delete = true;
                            total_items -= 1;
                        }
                        Layer2ScanFilterAction::AddBefore(y) => {
                            l1.get_mut(&y).unwrap().before_position = pre_buf_pos as u64;
                        }
                        Layer2ScanFilterAction::AddAfter(y) => {
                            l1.get_mut(&y).unwrap().after_position = pre_buf_pos as u64;
                        }
                    }
                }
            },
            _ => (),
        }
        xml_buf.clear();
    }

    pb.finish();
    println!("Loaded {total_items} items in scan-filter!");

    total_items
}

fn layer2_scan_filter(attrs: BytesStart, l1: &BTreeMap<i64, QInfo>) -> Layer2ScanFilterAction {
    let mut checks = 0;
    const REQUIRED_CHECKS : i32 = 5;

    let mut post_id = -1;
    let mut user_id = -1;
    let mut is_original = false;

    let attrs = attrs.attributes();

    for attr in attrs.map(Result::unwrap) {
        let attr_key = attr.key.as_ref();
        let attr_val = attr.value.as_ref();

        match attr_key {
            b"PostId" => {
                post_id = i64::from_str(std::str::from_utf8(attr_val).unwrap()).unwrap();
                if !l1.contains_key(&post_id) {
                    return Layer2ScanFilterAction::Ignore;
                }
                if l1[&post_id].delete {
                    return Layer2ScanFilterAction::Ignore;
                }
                checks += 1;
            }
            b"PostHistoryTypeId" => {
                match attr_val {
                    | b"2" => { // original post
                        is_original = true;
                    }
                    | b"5" => { // edit post
                        // no-op
                    }
                    | b"1"  // original title
                    | b"4"  // edit title
                    | _ => { return Layer2ScanFilterAction::Ignore; }
                }
                checks += 1;
            }
            b"ContentLicense" => {
                match attr_val {
                    | b"CC BY-SA 2.5"
                    | b"CC BY-SA 3.0"
                    | b"CC BY-SA 4.0"
                    => {}
                    _ => {
                        panic!("Not CC BY-SA 2.5/3.0/4.0! Found {}",
                               std::str::from_utf8(attr_val).unwrap());
                    }
                }
                checks += 1;
            }
            b"UserId" => {
                user_id = i32::from_str(std::str::from_utf8(attr_val).unwrap()).unwrap();
                checks += 1;
            }
            b"Text" => {
                checks += 1;
            }
            _ => (),
        }

        if checks == REQUIRED_CHECKS {
            break;
        }
    }

    if checks == REQUIRED_CHECKS {
        if is_original {
            Layer2ScanFilterAction::AddBefore(post_id)
        } else {
            if l1[&post_id].author_id == user_id {
                // wrong author! ergo more than one edit, delete
                Layer2ScanFilterAction::Delete(post_id)
            } else if l1[&post_id].after_position != u64::MAX {
                // too many edits, delete
                Layer2ScanFilterAction::Delete(post_id)
            } else {
                Layer2ScanFilterAction::AddAfter(post_id)
            }
        }
    } else {
        Layer2ScanFilterAction::Ignore
    }
}

fn layer2_generate(args: &Layer2Args, l1: &BTreeMap<i64, QInfo>, scan_count: u64) {
    let mut writer = {
        let underlying_stream = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&args.outfile)
            .expect("Failed to open OUTFILE for writing");
        BufWriter::new(underlying_stream)
    };

    let mut reader = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(&args.infile)
            .expect("Failed to open INFILE for reading")
    );

    println!("Extracting results and writing to {}", args.outfile.display());

    let pb = crate::progress_bar(scan_count);

    let mut out_count = l1.len() as u64;

    for (post_id, qinfo) in l1 {
        pb.inc(1);
        if qinfo.delete || qinfo.before_position == u64::MAX || qinfo.after_position == u64::MAX {
            out_count -= 1;
            continue
        }
        let before = layer2_load_revision(qinfo.before_position, &mut reader);
        let after = layer2_load_revision(qinfo.after_position, &mut reader);

        if before.is_none() || after.is_none() {
            out_count -= 1;
            continue;
        }

        let before = before.unwrap();
        let after = after.unwrap();

        let detabulated_before_text = before.text.replace("\t"," ");
        let detabulated_after_text = after.text.replace("\t", " ");

        writeln!(writer, "{}\t{}\t{}\t{}\t{}",
            post_id,
            detabulated_before_text,
            before.date.format(crate::DATE_FORMAT),
            detabulated_after_text,
            after.date.format(crate::DATE_FORMAT)
        ).unwrap();
        if writer.buffer().len() >= args.flush_interval {
            writer.flush().unwrap();
        }
    }

    writer.flush().unwrap();
    pb.finish();

    println!("Finished writing. Found {out_count} candidate revision pairs.");
}

struct Revision {
    date: NaiveDateTime,
    text: String,
}

fn layer2_load_revision(
    position: u64,
    reader: &mut BufReader<File>
) -> Option<Revision> {
    reader.seek(SeekFrom::Start(position + 1)).unwrap();
    let mut str_buf = String::new();
    reader.read_line(&mut str_buf).unwrap();
    let mut xml_reader = Reader::from_str(&str_buf);
    loop {
        match xml_reader.read_event() {
            Err(e) => panic!("Hit error {e} while parsing BUFFER {str_buf}"),
            Ok(Event::Eof) => panic!("Hit end-of-document for BUFFER {str_buf} at position {position}"),
            Ok(Event::Empty(elm)) => {
                assert_eq!(elm.name().as_ref(), b"row");
                let attrs = elm.attributes();
                let mut text = None;
                let mut date = None;
                for attr in attrs.map(Result::unwrap) {
                    match attr.key.as_ref() {
                        b"Text" => {
                            text = Some(String::from_utf8(attr.value.as_ref().to_vec()).unwrap());
                        }
                        b"CreationDate" => {
                            let s = std::str::from_utf8(attr.value.as_ref()).unwrap();
                            date = NaiveDateTime::parse_from_str(s, crate::DATE_FORMAT).ok();
                        }
                        _ => {}
                    }
                    if text.is_some() && date.is_some() {
                        break
                    }
                }
                if text.is_none() {
                    return None
                }
                return Some(Revision {
                    date: date.unwrap(),
                    text: text.unwrap(),
                })
            }
            _ => {}
        }
    }
}

pub fn layer2_filter(args: &Layer2Args) {
    let mut l1 = load_layer_1(args);

    let scan_count = layer2_scan(args, &mut l1);

    layer2_generate(args, &mut l1, scan_count);
}
