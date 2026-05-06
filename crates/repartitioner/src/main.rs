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
    if !plan.metadata.rewrite_required {
        statistics.set_after_partition_sizes(
            statistics.metadata.estimates.before_partition_sizes.clone(),
        );
        writer::write_no_op_output(
            &config.dataset.output,
            &plan.metadata,
            &statistics.metadata,
            &dataset,
        )?;
        return Ok(());
    }

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
