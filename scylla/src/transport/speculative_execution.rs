use futures::{
    future::FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use std::{future::Future, sync::Arc, time::Duration};
use tracing::warn;

use super::{errors::QueryError, metrics::Metrics};

/// The policy that decides if the driver will send speculative queries to the
/// next hosts when the current host takes too long to respond.
pub trait SpeculativeExecutionPolicy: Send + Sync {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    fn max_retry_count(&self) -> usize;

    /// The delay between each speculative execution
    fn retry_interval(&self) -> Duration;
}

/// A SpeculativeExecutionPolicy that schedules a given number of speculative
/// executions, separated by a fixed delay.
#[derive(Debug, Clone)]
pub struct SimpleSpeculativeExecutionPolicy {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    pub max_retry_count: usize,

    /// The delay between each speculative execution
    pub retry_interval: Duration,
}

/// A policy that triggers speculative executions when the request to the current
/// host is above a given percentile.
#[derive(Debug, Clone)]
pub struct PercentileSpeculativeExecutionPolicy {
    /// The maximum number of speculative executions that will be triggered
    /// for a given request (does not include the initial request)
    pub max_retry_count: usize,

    /// The percentile that a request's latency must fall into to be considered
    /// slow (ex: 99.0)
    pub percentile: f64,

    /// The component that will provide information about the latencies
    /// It can be acquired using `session.get_metrics()` method
    pub metrics: Arc<Metrics>,
}

impl SpeculativeExecutionPolicy for SimpleSpeculativeExecutionPolicy {
    fn max_retry_count(&self) -> usize {
        self.max_retry_count
    }

    fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl SpeculativeExecutionPolicy for PercentileSpeculativeExecutionPolicy {
    fn max_retry_count(&self) -> usize {
        self.max_retry_count
    }

    fn retry_interval(&self) -> Duration {
        let interval = self.metrics.get_latency_percentile_ms(self.percentile);
        let ms = match interval {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Failed to get latency percentile ({}), defaulting to 100 ms",
                    e
                );
                100
            }
        };
        Duration::from_millis(ms)
    }
}

pub async fn execute<QueryFut, ResT>(
    policy: &dyn SpeculativeExecutionPolicy,
    query_runner_generator: impl Fn() -> QueryFut,
) -> Result<ResT, QueryError>
where
    QueryFut: Future<Output = Option<Result<ResT, QueryError>>>,
{
    let mut retries_remaining = policy.max_retry_count();
    let retry_interval = policy.retry_interval();

    let mut async_tasks = FuturesUnordered::new();
    async_tasks.push(query_runner_generator());

    let sleep = tokio::time::sleep(retry_interval).fuse();
    tokio::pin!(sleep);

    loop {
        futures::select! {
            _ = &mut sleep => {
                if retries_remaining > 0 {
                    async_tasks.push(query_runner_generator());
                    retries_remaining -= 1;

                    // reset the timeout
                    sleep.set(tokio::time::sleep(retry_interval).fuse());
                }
            }
            res = async_tasks.select_next_some() => {
                match res {
                    Some(r) => return r,
                    None =>  {
                        if async_tasks.is_empty() && retries_remaining == 0 {
                            return Err(QueryError::ProtocolError(
                                "Empty query plan - driver bug!",
                            ));
                        }
                        continue
                    },
                }
            }
        }
    }
}