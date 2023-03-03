use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str::FromStr;
use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Args;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

use crate::generate_question_index::load_question_index;
use crate::PostId;

#[derive(Args)]
pub struct GenerateRevisionSetArgs {
    #[arg(long = "in-history", required = true)]
    in_history: PathBuf,
    #[arg(long = "in-qidx")]
    in_question_index: PathBuf,
    #[arg(long = "out-file", required = true)]
    outfile: PathBuf,
    #[arg(long = "qcount", default_value_t = 0)]
    question_count: u64,
    #[arg(long = "pcount", default_value_t = 0)]
    post_count: u64,
    #[arg(long = "flush-interval", default_value_t = 1000000)]
    flush_interval: usize,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RevisionClass {
    Original = 2,
    Edit = 5,
    Rollback = 8,
    Undefined = 255,
}

#[derive(Debug)]
struct Revision {
    class: RevisionClass,
    position: u64,
}

#[derive(Debug)]
pub struct Detail {
    class: RevisionClass,
    text: String,
    date: DateTime<Utc>,
}

pub fn generate_revision_set(args: &GenerateRevisionSetArgs) {
    let qidx = load_question_index(
        &args.in_question_index,
        args.question_count);

    let underlying_stream = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.outfile)
        .expect("Failed to open OUTFILE for writing");
    let mut writer = BufWriter::new(underlying_stream);

    let (rev_map, tot_rev) = {
        let mut reader = Reader::from_file(&args.in_history)
            .expect("Failed to open IN_HISTORY for reading");
        load_revisions_as_indices(&mut reader, &qidx, args)
    };

    let hist_file = OpenOptions::new()
        .read(true)
        .open(&args.in_history)
        .expect("Failed to open IN_HISTORY for reading");
    let mut hist = BufReader::new(hist_file);

    println!("Retrieving results and writing to {}", args.outfile.display());
    let pb = ProgressBar::new(tot_rev as u64);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
    let mut multi_count = 0;
    for (post_id, revs) in rev_map {
        // println!("PID [ {post_id} ] => ");
        let mut details : Vec<Detail> = revs.into_iter().map(|rev| {
            pb.inc(1);
            detail_from_rev(rev, &mut hist)
        }).collect();
        // note: stable!
        details.sort_by_key(|x| x.date);
        // for w in details.windows(2) {
        //     assert!(w.len() == 2);
        if details.len() >= 2 {
            multi_count += 1;
            writeln!(writer, "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                     post_id,
                     details[0].class as i64,
                     details[0].date,
                     details[0].text,
                     details[details.len() - 1].class as i64,
                     details[details.len() - 1].date,
                     details[details.len() - 1].text,
            ).unwrap();
            if writer.buffer().len() >= args.flush_interval {
                writer.flush().unwrap();
            }
        }
        // }
    }
    writer.flush().unwrap();
    pb.finish();
    println!("Finished writing. Found {multi_count} revision pairs among {tot_rev} total history events!");
}

const DATE_FORMAT : &'static str = "%Y-%m-%dT%H:%M:%S%.3f";

fn detail_from_rev(rev: Revision, hist: &mut BufReader<File>) -> Detail {
    // println!("\t{rev:?}");
    // +1: skip the newline
    // apparently using hist.get_mut().seek() completely breaks here??
    hist.seek(SeekFrom::Start(rev.position+1)).unwrap();
    let mut str_buf = String::new();
    str_buf.clear();
    hist.read_line(&mut str_buf).unwrap();
    // println!("POSITION {} :: {}", rev.position, str_buf);
    let mut xml_reader = Reader::from_str(&str_buf);
    loop {
        match xml_reader.read_event() {
            Err(e) => panic!("Hit error {e} while parsing BUFFER {str_buf}"),
            Ok(Event::Eof) => panic!("Hit end-of-document for BUFFER {str_buf}"),
            Ok(Event::Empty(elm)) => {
                assert_eq!(elm.name().as_ref(), b"row");
                let attrs = elm.attributes();
                let mut text = None;
                let mut date = None;
                for attr in attrs {
                    let attr = attr.unwrap();
                    match attr.key.as_ref() {
                        b"Text" => {
                            text = Some(String::from_utf8(attr.value.as_ref().to_vec()).unwrap());
                        }
                        b"CreationDate" => {
                            let s = std::str::from_utf8(attr.value.as_ref()).unwrap();
                            let ndt = NaiveDateTime::parse_from_str(s, DATE_FORMAT).unwrap();
                            date = Some(DateTime::from_utc(ndt, Utc));
                        }
                        _ => (),
                    }
                    if text.is_some() && date.is_some() {
                        break
                    }
                }

                // OKAY so apparently if message body is empty??? there's just no Text attribute
                //
                //if text.is_none() || date.is_none() {
                //    panic!("Text and CreationDate attributes not present when expected: row is {str_buf}")
                //}
                let det = Detail {
                    class: rev.class,
                    text: text.unwrap_or("".to_string()),
                    date: date.unwrap(),
                };
                // println!("\t{det:?}");
                return det
            },
            _ => ()
        }
    }
}

fn load_revisions_as_indices(reader: &mut Reader<BufReader<File>>, qidx: &BTreeSet<PostId>, args: &GenerateRevisionSetArgs) -> (BTreeMap<PostId, Vec<Revision>>, usize) {
    let mut questions_with_revisions = BTreeMap::<PostId, Vec<Revision>>::new();
    let mut total_revisions = 0;

    let mut xml_buf = Vec::new();
    let pb = ProgressBar::new(args.post_count);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));

    println!("Loading question revisions from {}", args.in_history.display());
    loop {
        pb.inc(1);

        let pre_buf_pos = reader.buffer_position();

        match reader.read_event_into(&mut xml_buf) {
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            Ok(Event::Eof) => break,
            Ok(Event::Empty(element)) => {
                if element.name().as_ref() == b"row" {
                    if let Some((pid, rev)) = process_element(element, qidx, pre_buf_pos) {
                        match questions_with_revisions.entry(pid) {
                            Entry::Vacant(v) => {
                                v.insert(vec![rev]);
                            }
                            Entry::Occupied(mut o) => {
                                o.get_mut().push(rev);
                            }
                        }
                        total_revisions += 1;
                    }
                }
            }
            _ => (),
        }
        xml_buf.clear()
    }

    pb.finish();
    println!("Loaded {total_revisions} revisions across {} questions!",
             questions_with_revisions.len());

    (questions_with_revisions, total_revisions)
}

fn process_element(element: BytesStart, qidx: &BTreeSet<PostId>, buf_pos: usize) -> Option<(PostId, Revision)> {
    let attrs = element.attributes();
    let mut post_id = -1;
    let mut hist_type_id = RevisionClass::Undefined;
    let mut license = false;
    let mut has_text = false;
    // println!("=== ROW ===");
    for attr in attrs {
        let attr = attr.unwrap();
        // println!("{}={}", std::str::from_utf8(attr.key.as_ref()).unwrap(), std::str::from_utf8(attr.value.as_ref()).unwrap());
        match attr.key.as_ref() {
            b"PostId" => {
                post_id = i64::from_str(
                    std::str::from_utf8(attr.value.as_ref()).unwrap()
                ).unwrap();
                if !qidx.contains(&post_id) {
                    // not a question
                    return None;
                }
            }
            b"PostHistoryTypeId" => {
                match attr.value.as_ref() {
                    b"2" => hist_type_id = RevisionClass::Original,
                    b"5" => hist_type_id = RevisionClass::Edit,
                    b"8" => hist_type_id = RevisionClass::Rollback,
                    // not the history type we want
                    _ => return None
                }
            }
            b"ContentLicense" => {
                match attr.value.as_ref() {
                    | b"CC BY-SA 2.5"
                    | b"CC BY-SA 3.0"
                    | b"CC BY-SA 4.0"
                    => {}
                    _ => {
                        panic!("Not CC BY-SA 2.5/3.0/4.0! at {}: {}", buf_pos,
                               std::str::from_utf8(attr.value.as_ref()).unwrap())
                    }
                }
                license = true;
            }
            b"Text" => {
                has_text = true;
            }
            _ => (),
        }
        if has_text && post_id != -1 && hist_type_id != RevisionClass::Undefined && license {
            // println!("------------------------> OK!");
            return Some((post_id, Revision {
                class: hist_type_id,
                position: buf_pos as u64,
            }));
        }
    }
    None
}