use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{PathBuf};
use std::str::FromStr;
use clap::Args;
use indicatif::ProgressIterator;
use crate::{PostId, progress_bar};

#[derive(Args)]
pub struct RelabelCodeArgs {
    #[arg(long = "in-revision-set", required = true)]
    in_revision_set: PathBuf,
    #[arg(long = "out-file", required = true)]
    outfile: PathBuf,
    #[arg(long = "rcount", default_value_t = 0)]
    revision_count: u64,
    #[arg(long = "flush-interval", default_value_t = 1000000)]
    flush_interval: usize,
}

#[derive(Debug)]
struct RDelta {
    post_id: PostId,
    text_before: String,
    text_after: String,
}

// [a,b]
#[derive(Debug, Copy, Clone)]
struct Span {
    start: usize,
    end: usize,
    tag: i32,
}

fn locate_code_segments(s: &str) -> Vec<Span> {
    let s = s.as_bytes();
    let mut s4_spans = vec![];

    {
        let mut mark = 0;
        let mut did_start = false;
        for i in 0..s.len() {
            if s[i] == b'\n' {
                if i + 4 < s.len()
                    && s[i + 1] == b' '
                    && s[i + 2] == b' '
                    && s[i + 3] == b' '
                    && s[i + 4] == b' '
                {
                    if !did_start {
                        did_start = true;
                        mark = i + 1;
                    }
                } else {
                    if did_start {
                        did_start = false;
                        s4_spans.push(Span {
                            start: mark,
                            end: i-1,
                            tag: -1,
                        })
                    }
                }
            }
        }
    }

    let mut spans = vec![];
    {
        let mut mark = 0;
        let mut did_start = false;
        let mut triple = false;
        let mut trailing_triple = 0;
        let mut j = 0;
        for i in 0..s.len() {
            if j < s4_spans.len() {
                if i > s4_spans[j].end {
                    j += 1;
                }
                if j < s4_spans.len() {
                    if i >= s4_spans[j].start && i <= s4_spans[j].end {
                        continue;
                    }
                }
            }
            if trailing_triple > 0 {
                trailing_triple -= 1;
                continue;
            }
            if i > 0 && s[i - 1] == b'\n' {
                if i + 2 < s.len()
                    && s[i] == b'`'
                    && s[i + 1] == b'`'
                    && s[i + 2] == b'`'
                {
                    if triple && did_start {
                        triple = false;
                        did_start = false;
                        spans.push(Span {
                            start: mark,
                            end: i + 2,
                            tag: -1,
                        });
                        trailing_triple = 2;
                    } else if did_start {
                        did_start = false;
                        spans.push(Span {
                            start: mark,
                            end: i,
                            tag: -1,
                        })
                    } else {
                        triple = true;
                        did_start = true;
                        mark = i;
                    }
                    // IMPORTANT
                    continue;
                }
            }
            if s[i] == b'`' && (i == 0 || s[i - 1] != b'\\') {
                if !triple {
                    if !did_start {
                        mark = i;
                        did_start = true;
                    } else {
                        did_start = false;
                        spans.push(Span {
                            start: mark,
                            end: i,
                            tag: -1,
                        });
                    }
                }
            }
        }
    }
    s4_spans.append(&mut spans);
    s4_spans
}

fn apply_spans(t: &str, spans: &[Span]) -> String {
    let mut end_prev_span = 0;
    let mut buf = String::new();
    for (i, span) in spans.iter().enumerate() {
        if span.start > end_prev_span {
            buf.push_str(&t[end_prev_span..span.start]);
        }
        buf.push_str(&format!("<N{i}>"));
        end_prev_span = span.end + 1;
    }
    buf.push_str(&t[end_prev_span..]);
    buf
}

pub fn relabel_code(args: &RelabelCodeArgs) {
    println!("Relabeling revision deltas from {}", args.in_revision_set.display());
    let pb = progress_bar(args.revision_count);

    let f = OpenOptions::new()
        .read(true)
        .open(&args.in_revision_set)
        .expect("Failed to open revision deltas for reading");
    let reader = BufReader::new(f);
    let mut tot_input = 0;
    let mut tot_filtered = 0;
    let underlying_stream = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.outfile)
        .expect("Failed to open OUTFILE for writing");
    let mut writer = BufWriter::new(underlying_stream);
    reader.lines()
        .progress_with(pb.clone())
        .map(|line| {
            let line = line.unwrap();
            let mut frags = line.split('\t');
            let post_id = PostId::from_str(frags.next().unwrap()).unwrap();
            frags.next().unwrap();
            frags.next().unwrap();
            let text_before = frags.next().unwrap().to_string();
            frags.next().unwrap();
            frags.next().unwrap();
            let text_after = frags.next().unwrap().to_string();
            RDelta {
                post_id,
                text_before,
                text_after,
            }
        })
        .for_each(|rdelta| {
            // find code segments
            // specifically, anything inside of backticks `` is code
            // anything between `&#xD;&#xA;    ` and `&#xD;&#xA;` is code
            // anything between `&#xD;&#xA;\`\`\`` and `&#xD;&#xA;\`\`\`` is code
            // UNLESS, of course, the backticks are escaped
            // or someone didn't put in the correct number of backticks
            let text_before = rdelta.text_before.replace("&#xD;&#xA;", "\n");
            let text_after = rdelta.text_after.replace("&#xD;&#xA;", "\n");
            let b_before = text_before.as_bytes();
            let b_after = text_after.as_bytes();

            let mut spans_before = locate_code_segments(&text_before);
            let mut spans_after = locate_code_segments(&text_after);
            // println!("Before: {spans_before:?}");
            // println!("After: {spans_after:?}");
            let mut unified_spans = vec![];
            for i in 0..spans_before.len() {
                let span = spans_before[i];
                unified_spans.push((0, i, &b_before[span.start..=span.end]));
            }
            for i in 0..spans_after.len() {
                let span = spans_after[i];
                unified_spans.push((1, i, &b_after[span.start..=span.end]));
            }
            unified_spans.dedup_by_key(|x| x.2);
            // println!("Unified: {unified_spans:?}");

            for i in 0..spans_before.len() {
                let span = spans_before[i];
                let k = unified_spans.iter()
                    .position(|uspan| uspan.2 == &b_before[span.start..=span.end])
                    .unwrap();
                spans_before[i].tag = k as i32;
            }
            for i in 0..spans_after.len() {
                let span = spans_after[i];
                let k = unified_spans.iter()
                    .position(|uspan| uspan.2 == &b_after[span.start..=span.end])
                    .unwrap();
                spans_after[i].tag = k as i32;
            }

            let relabeled_before = apply_spans(&text_before, &spans_before);
            let relabeled_after = apply_spans(&text_after, &spans_after);
            // println!("========================================= Original before:\n {text_before}");
            // println!("========================================= Original after:\n {text_after}");
            // println!("========================================= Relabeled before\n: {relabeled_before}");
            // println!("========================================= Relabeled after\n: {relabeled_after}");
            let relabeled_before = relabeled_before.replace("\n", "&#xD;&#xA;");
            let relabeled_after = relabeled_after.replace("\n", "&#xD;&#xA;");

            if relabeled_before != relabeled_after {
                write!(writer, "{relabeled_before}\t{relabeled_after}\n").unwrap();
                if writer.buffer().len() > args.flush_interval {
                    writer.flush().unwrap();
                }
                tot_filtered += 1;
            }
            tot_input += 1;
        });
    writer.flush().unwrap();
    pb.finish();
    println!("Loaded {tot_input} revision deltas, kept {tot_filtered}");
}
