use std::process::ExitCode;

use repartitioner::{cli::CliArgs, partitioner, planner, reader, statistics, writer, Result};

fn main() -> ExitCode {
    match run(CliArgs::parse_args()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: CliArgs) -> Result<()> {
    let config = args.load_config()?;
    let dataset = reader::read_dataset(&config)?;
    let mut statistics = statistics::compute_statistics(&config, &dataset)?;
    let plan = planner::build_plan(&config, &statistics)?;
    let assignments = partitioner::assign_partitions(&plan, &dataset)?;
    statistics.set_after_partition_sizes(assignments.partition_row_counts.clone());

    writer::write_output(
        &config.dataset.output,
        &plan.metadata,
        &statistics.metadata,
        &assignments,
        &dataset,
    )?;

    Ok(())
}
