use core::slice::SlicePattern;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use clap::Args;
use quick_xml::events::Event;
use quick_xml::Reader;

#[derive(Args)]
pub struct Layer1Args {
    #[arg(long = "in-file", required=true)]
    infile: PathBuf,
    #[arg(long = "out-file", required=true)]
    outfile: PathBuf,
    #[arg(long = "pcount", default_value_t=115443102)]
    p_count: u64,
    #[arg(long = "flush-interval", default_value_t=1_000_000)]
    flush_interval: usize,
}

pub fn layer1_filter(args: &Layer1Args) {
    let mut reader = Reader::from_file(&args.infile)
        .expect("Failed to open INFILE for reading");
    let underlying_stream = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.outfile)
        .expect("Failed to open OUTFILE for writing");
    let mut writer = BufWriter::new(underlying_stream);
    let bar = crate::progress_bar(args.p_count);

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
                    let mut author_id = -1;
                    let mut last_editor_user_id = -1;
                    let mut required_fields = 0;
                    const REQUIRED_CHECKS: i32 = 4;
                    for attr in attrs.map(Result::unwrap) {
                        let attr_key = attr.key.as_ref();
                        if attr_key == b"Id" {
                            required_fields += 1;
                            post_id = i64::from_str(std::str::from_utf8(attr.value.as_slice()).unwrap()).unwrap();
                        }
                        if attr_key == b"PostTypeId" {
                            if attr.value.as_ref() == b"1" {
                                required_fields += 1;
                            } else {
                                break
                            }
                        }
                        if attr_key == b"OwnerUserId" {
                            author_id = i32::from_str(std::str::from_utf8(attr.value.as_slice()).unwrap()).unwrap();
                            required_fields += 1;
                        }
                        if attr_key == b"LastEditorUserId" {
                            last_editor_user_id = i32::from_str(std::str::from_utf8(attr.value.as_slice()).unwrap()).unwrap();
                            required_fields += 1;
                        }
                        if required_fields == REQUIRED_CHECKS {
                            break
                        }
                    }
                    debug_assert!(required_fields <= REQUIRED_CHECKS);
                    if required_fields == REQUIRED_CHECKS
                        && last_editor_user_id != author_id
                    {
                        writeln!(writer, "{post_id}\t{author_id}").unwrap();
                        if writer.buffer().len() >= args.flush_interval {
                            writer.flush().unwrap();
                        }
                    }
                }
            }
            _ => (),
        }
        buf.clear();
    }

    writer.flush().unwrap();
}