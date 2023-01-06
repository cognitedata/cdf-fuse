use clap::{Arg, ArgAction, Command};
use fs::CdfFS;
use fuser::MountOption;

mod err;
mod fs;
mod sync;

fn main() {
    env_logger::init();
    let matches = Command::new("CDF FileSystem")
        .arg(
            Arg::new("mount-point")
                .long("mount-point")
                .value_name("MOUNT_POINT")
                .default_value("")
                .help("Mount CDF Fuse at the given point")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("config")
                .long("config")
                .value_name("CONFIG")
                .default_value("config.json")
                .help("Path to JSON configuration file")
                .action(ArgAction::Set),
        )
        .get_matches();

    let mount_point = matches
        .get_one::<String>("mount-point")
        .cloned()
        .unwrap_or_default();

    let options = vec![
        MountOption::FSName("cdffs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
    ];
    let fs = CdfFS::new(
        &matches
            .get_one::<String>("config")
            .cloned()
            .unwrap_or_else(|| "config.json".to_string()),
    );
    fuser::mount2(fs, mount_point, &options).unwrap();
}
