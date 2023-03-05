use clap::{AppSettings, ArgEnum, Parser};

use trade_archivist::archivist::{run_archivist, ExchangeName};
use trade_archivist::exchange::binance::Binance;
use trade_archivist::exchange::ExchangeAPI;
use trade_archivist::order_book;

#[derive(Clone, ArgEnum)]
enum ExchangeArgList {
    Binance,
    Bittrex,
}

impl ExchangeArgList {
    fn exchange_plugin(&self) -> ExchangeName {
        match self {
            ExchangeArgList::Binance => ExchangeName::Binance,
            ExchangeArgList::Bittrex => ExchangeName::Bittrex,
        }
    }
}

#[derive(clap::Subcommand)]
enum Commands {
    Archive {
        #[clap(arg_enum)]
        exchange: ExchangeArgList,
        #[clap(long)]
        storage: String,
        #[clap(long)]
        listen: String,
        #[clap(long)]
        old_storage: Vec<String>,
    },
}

#[derive(Parser)]
#[clap(name = "trade-archivist")]
#[clap(author = "Kirill Mavreshko <kimavr@gmail.com>")]
#[clap(about = "Collects and provides access to trading history")]
#[clap(version = "0.0001")]
#[clap(global_setting(AppSettings::PropagateVersion))]
#[clap(global_setting(AppSettings::UseLongFormatForHelpSubcommand))]
#[clap(global_setting(AppSettings::ArgRequiredElseHelp))]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

fn configure_logging() {
    let config = simplelog::ConfigBuilder::default()
        .set_time_format("%+".to_string())
        .build();
    simplelog::CombinedLogger::init(vec![simplelog::TermLogger::new(
        simplelog::LevelFilter::Debug,
        config,
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Auto,
    )])
    .expect("Logging should be properly configured");
    log::info!("Logging is configured");
}

fn main() {
    configure_logging();
    let cli: Cli = Cli::parse();
    match cli.command {
        Commands::Archive {
            exchange,
            storage,
            listen,
            old_storage,
        } => {
            let pairs = Binance::trading_pairs();
            let mut navigator = order_book::navigation::StorageNavigator::new(pairs.as_slice());
            for dir in &old_storage {
                navigator.add_dir(dir, true);
            }
            navigator.add_dir(&storage, false);
            navigator.rescan(); // Saves time on re-scanning static directories later
            run_archivist(exchange.exchange_plugin(), storage, listen, navigator);
        }
    }
}
