#![feature(slice_pattern)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

mod generate_question_index;
mod generate_revision_set;
mod relabel_code;

use std::fmt;
use std::time::Duration;
use clap::{Parser, Subcommand};
use indicatif::{HumanDuration, ProgressBar, ProgressState, ProgressStyle};
use crate::generate_revision_set::{generate_revision_set, GenerateRevisionSetArgs};
use crate::generate_question_index::{generate_question_index, GenerateQuestionIndexArgs};
use crate::relabel_code::{relabel_code, RelabelCodeArgs};

pub type PostId = i64;

enum PostTypeId {
    Question = 1,
}

fn progress_bar(count: u64) -> ProgressBar {
    let pb = ProgressBar::new(count);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
    pb
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[clap(name="generate-question-index")]
    GenerateQuestionIndex(GenerateQuestionIndexArgs),
    #[clap(name="generate-revision-set")]
    GenerateRevisionSet(GenerateRevisionSetArgs),
    #[clap(name="relabel-code")]
    RelabelCode(RelabelCodeArgs),
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::GenerateQuestionIndex(args) => {
            generate_question_index(args)
        }
        Commands::GenerateRevisionSet(args) => {
            generate_revision_set(args)
        }
        Commands::RelabelCode(args) => {
            relabel_code(args)
        }
    }
}
