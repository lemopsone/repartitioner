use crate::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionResources {
    pub local_threads: usize,
    pub memory_limit_mb: u64,
    pub parallel_execution_enabled: bool,
}

impl ExecutionResources {
    pub fn from_config(config: &Config) -> Self {
        Self {
            local_threads: config.resources.local_threads.max(1),
            memory_limit_mb: config.resources.memory_limit_mb,
            parallel_execution_enabled: false,
        }
    }

    pub fn open_writer_limit(&self) -> usize {
        self.local_threads.max(1)
    }
}
