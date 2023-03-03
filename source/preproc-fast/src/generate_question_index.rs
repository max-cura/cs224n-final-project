use core::slice::SlicePattern;
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use indicatif::{ProgressBar, ProgressIterator, ProgressState, ProgressStyle};
use quick_xml::events::Event;
use quick_xml::Reader;
use clap::Args;
use crate::{PostId, progress_bar};

#[derive(Args)]
pub struct GenerateQuestionIndexArgs {
    #[arg(long = "in-file", required=true)]
    infile: PathBuf,
    #[arg(long = "out-file", required=true)]
    outfile: PathBuf,
    #[arg(long = "tcount")]
    tcount: u64,
    #[arg(long = "flush-interval", default_value_t=2048)]
    flush_interval: u64,
}
pub fn generate_question_index(args: &GenerateQuestionIndexArgs) {
    let mut reader = Reader::from_file(&args.infile)
        .expect("Failed to open INFILE for reading");
    let underlying_stream = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.outfile)
        .expect("Failed to open OUTFILE for writing");
    let mut writer = std::io::BufWriter::new(underlying_stream);
    let bar = ProgressBar::new(args.tcount);
    bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
    let mut bufcount = 0;

    let mut buf = Vec::new();

    loop {
        bar.inc(1);
        match reader.read_event_into(&mut buf) {
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            Ok(Event::Eof) => break,
            Ok(Event::Empty(element)) => {
                if element.name().as_ref() == b"row" {
                    let attrs = element.attributes();
                    let mut post_id = -1;
                    let mut type_checked = false;
                    for attr in attrs {
                        let attr = attr.unwrap();
                        if attr.key.as_ref() == b"Id" {
                            post_id = i64::from_str(
                                std::str::from_utf8(attr.value.as_slice()
                                ).unwrap()
                            ).unwrap();
                        }
                        if attr.key.as_ref() == b"PostTypeId" {
                            if attr.value.as_ref() == b"1" {
                                type_checked = true;
                            } else {
                                post_id = -1;
                                break
                            }
                        }
                        if type_checked && post_id != -1 {
                            break
                        }
                    }
                    if type_checked {
                        writeln!(writer, "{post_id}").unwrap();
                        bufcount += 1;
                        if bufcount == args.flush_interval {
                            writer.flush().unwrap();
                            bufcount = 0;
                        }
                    }
                }
            }
            _ => (),
        }
        buf.clear();
    }

    writer.flush().unwrap();

    bar.finish_with_message("done");
}

pub fn load_question_index(
    from_file: &Path,
    question_count: u64,
) -> BTreeSet<PostId> {
    println!("Loading question index from {}", from_file.display());
    let pb = progress_bar(question_count);

    let f = OpenOptions::new()
        .read(true)
        .open(from_file)
        .expect("Failed to open question index for reading");
    let reader = BufReader::new(f);
    let set =
        reader.lines()
            .progress_with(pb.clone())
            .map(|line|
                PostId::from_str(line.expect("failed to read line").as_str())
                    .unwrap()
            )
            .collect::<BTreeSet<PostId>>();
    pb.finish();
    println!("Loaded {} question indices", set.len());
    set
}

