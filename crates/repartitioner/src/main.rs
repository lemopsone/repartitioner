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
    let mut config = args.load_config()?;
    apply_join_inputs(&mut config);

    let read_started = Instant::now();
    let dataset = reader::read_dataset(&config)?;
    let read_seconds = elapsed_seconds(read_started);

    let statistics_started = Instant::now();
    let mut statistics = statistics::compute_statistics(&config, &dataset)?;
    attach_join_statistics(&config, &mut statistics)?;
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

fn apply_join_inputs(config: &mut repartitioner::Config) {
    if config.job.job_type != repartitioner::config::JobType::Join {
        return;
    }

    let Some(join_config) = &config.join else {
        return;
    };

    config.dataset.input = join_config.left_input.clone();
    config.partitioning.key_columns = join_config.join_keys.clone();
}

fn attach_join_statistics(
    config: &repartitioner::Config,
    computed_statistics: &mut statistics::ComputedStatistics,
) -> Result<()> {
    if config.job.job_type != repartitioner::config::JobType::Join {
        return Ok(());
    }

    let Some(join_config) = &config.join else {
        return Ok(());
    };

    let mut right_config = config.clone();
    right_config.dataset.input = join_config.right_input.clone();
    right_config.partitioning.key_columns = join_config.join_keys.clone();
    let right_dataset = reader::read_dataset(&right_config)?;
    let right_statistics = statistics::compute_statistics(&right_config, &right_dataset)?;
    computed_statistics.set_join_statistics(statistics::build_join_statistics(
        join_config.join_keys.clone(),
        computed_statistics,
        Some(&right_statistics),
    ));

    Ok(())
}
