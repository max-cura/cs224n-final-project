#![feature(slice_pattern)]

extern crate core;

mod layer_1;
mod layer_2;
mod layer_3;
mod layer_4;

use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

pub type PostId = i64;
pub const DATE_FORMAT : &'static str = "%Y-%m-%dT%H:%M:%S%.3f";

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
    #[clap(name="layer1")]
    Layer1(layer_1::Layer1Args),
    #[clap(name="layer2")]
    Layer2(layer_2::Layer2Args),
    #[clap(name="layer3")]
    Layer3(layer_3::Layer3Args),
    #[clap(name="layer4")]
    Layer4(layer_4::Layer4Args),
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Layer1(args) => {
            layer_1::layer1_filter(args);
        }
        Commands::Layer2(args) => {
            layer_2::layer2_filter(args);
        }
        Commands::Layer3(args) => {
            layer_3::layer3_filter(args);
        }
        Commands::Layer4(args) => {
            layer_4::layer4_filter(args);
        }
    }
}
