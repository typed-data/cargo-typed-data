use clap::{Arg, ArgMatches, Command};

mod schema_to_code;
mod hf;



fn main() -> Result<(), Box<dyn std::error::Error>> {
    match parse_cli() {
        Ok(matches) => {
            if let Some(from_file) = matches.get_one::<String>("from-file") {
                schema_to_code::from_file(from_file)?;
            } else if let Some(from_hf) = matches.get_one::<String>("from-hf") {
                let (file,_) = hf::from_hf(from_hf)?;
                schema_to_code::from_file(&file)?;
            }
        }
        Err(e) => {
            eprintln!("Error parsing CLI: {}", e);
        }
    }

    Ok(())
}

fn parse_cli() -> Result<ArgMatches, clap::Error> {
    Command::new("bodkin-gen")
        .arg(
            Arg::new("from-file")
                .long("from-file")
                .value_name("FILE")
                .help("Generate code from local schema file"),
        )
        .arg(
            Arg::new("from-hf")
                .long("from-hf")
                .value_name("MODEL")
                .help("Generate code from Hugging Face schema"),
        )
        .try_get_matches()
}
