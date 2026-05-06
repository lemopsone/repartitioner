use std::{process::ExitCode, time::Instant};

use repartitioner::{
    cli::CliArgs, manifest::TimingMetadata, partitioner, planner, reader, statistics, writer,
    Result,
};

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
    let total_started = Instant::now();
    let config = args.load_config()?;

    let read_started = Instant::now();
    let dataset = reader::read_dataset(&config)?;
    let read_seconds = elapsed_seconds(read_started);

    let statistics_started = Instant::now();
    let mut statistics = statistics::compute_statistics(&config, &dataset)?;
    let statistics_seconds = elapsed_seconds(statistics_started);

    let planning_started = Instant::now();
    let plan = planner::build_plan(&config, &statistics)?;
    let planning_seconds = elapsed_seconds(planning_started);

    if !plan.metadata.rewrite_required {
        statistics.set_after_partition_sizes(
            statistics.metadata.estimates.before_partition_sizes.clone(),
        );
        let writing_started = Instant::now();
        let write_summary = writer::write_no_op_output(
            &config.dataset.output,
            &plan.metadata,
            &statistics.metadata,
            &dataset,
        )?;
        let writing_seconds = elapsed_seconds(writing_started);
        statistics.set_timing(TimingMetadata {
            read_seconds,
            statistics_seconds,
            planning_seconds,
            assignment_seconds: 0.0,
            writing_seconds,
            total_seconds: elapsed_seconds(total_started),
        });
        writer::write_metadata_files(
            &config.dataset.output,
            &plan.metadata,
            &statistics.metadata,
            &write_summary.manifest,
        )?;
        return Ok(());
    }

    let assignment_started = Instant::now();
    let assignments = partitioner::assign_partitions(&plan, &dataset)?;
    let assignment_seconds = elapsed_seconds(assignment_started);
    statistics.set_after_partition_sizes(assignments.partition_row_counts.clone());

    let writing_started = Instant::now();
    let write_summary = writer::write_output(
        &config.dataset.output,
        &plan.metadata,
        &statistics.metadata,
        &assignments,
        &dataset,
    )?;
    let writing_seconds = elapsed_seconds(writing_started);
    statistics.set_timing(TimingMetadata {
        read_seconds,
        statistics_seconds,
        planning_seconds,
        assignment_seconds,
        writing_seconds,
        total_seconds: elapsed_seconds(total_started),
    });
    writer::write_metadata_files(
        &config.dataset.output,
        &plan.metadata,
        &statistics.metadata,
        &write_summary.manifest,
    )?;

    Ok(())
}

fn elapsed_seconds(started: Instant) -> f64 {
    started.elapsed().as_secs_f64()
}
