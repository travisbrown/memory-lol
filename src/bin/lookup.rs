use clap::Parser;
use memory_lol::{cli, error::Error, lookup::Lookup, report};

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = cli::init_logging(opts.verbose);

    match opts.command {
        SubCommand::ScreenName { screen_name } => {
            let db = Lookup::new(opts.db)?;
            let result = db.lookup_screen_name(&screen_name)?;
            println!("{:?}", result);
        }
        SubCommand::Tweet { status_id } => {
            let db = Lookup::new(opts.db)?;
            let result = db.lookup_tweet_metadata(status_id)?;
            println!("{:?}", result);
        }
        SubCommand::User { user_id } => {
            let db = Lookup::new(opts.db)?;
            let result = db.lookup_user(user_id)?;
            println!("{}", serde_json::to_string(&result)?);
        }
        SubCommand::UserReport { user_id } => {
            let db = Lookup::new(opts.db)?;
            let report = report::generate_user_report(&db, user_id)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        SubCommand::Deletes { user_id } => {
            let db = Lookup::new(opts.db)?;
            let result = db.lookup_deletes(user_id)?;
            println!("{:?}", result);
        }
        SubCommand::Stats => {
            let db = Lookup::new(opts.db)?;
            println!("{:?}", db.get_stats()?);
        }
        SubCommand::Files => {
            let db = Lookup::new(opts.db)?;
            let completed_files = db.get_completed_files()?;

            for (archive_path, file_path, status_count) in completed_files {
                println!("{}, {}: {}", archive_path, file_path, status_count);
            }
        }
        SubCommand::DumpUserIds => {
            let db = Lookup::new(opts.db)?;

            for user_id in db.get_user_ids()? {
                println!("{}", user_id);
            }
        }
        SubCommand::DumpFullStatusIds => {
            let db = Lookup::new(opts.db)?;

            for status_id in db.get_full_status_ids()? {
                println!("{}", status_id);
            }
        }
        SubCommand::DumpShortStatusIds => {
            let db = Lookup::new(opts.db)?;

            for status_id in db.get_short_status_ids()? {
                println!("{}", status_id);
            }
        }
        SubCommand::DumpDeleteIds => {
            let db = Lookup::new(opts.db)?;

            for status_id in db.get_delete_ids()? {
                println!("{}", status_id);
            }
        }
        SubCommand::ExtendedUser { user_id } => {
            let db = memory_lol::extended::UserLookup::new(opts.db)?;
            let results = db.lookup_user(user_id)?;

            for result in results {
                println!("{:?}", result);
            }
        }
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "lookup", about, version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Path to the RocksDB directory
    #[clap(short, long, default_value = "tmp/data/all")]
    db: String,
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    ScreenName {
        /// Screen name
        screen_name: String,
    },
    Tweet {
        /// Status ID
        status_id: u64,
    },
    User {
        /// User ID
        user_id: u64,
    },
    UserReport {
        /// User ID
        user_id: u64,
    },
    Deletes {
        /// User ID
        user_id: u64,
    },
    Stats,
    Files,
    DumpUserIds,
    DumpFullStatusIds,
    DumpShortStatusIds,
    DumpDeleteIds,
    ExtendedUser {
        /// User ID
        user_id: u64,
    },
}
