use std::future::Future;
use std::mem;
use std::result::Result;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Mutex};

use bytes::Bytes;
use futures::future::{Abortable, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{future::AbortHandle, Stream};
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{errors::QueryError, speculative_execution::SpeculativeExecutionPolicy};
use crate::cql_to_rust::{FromRow, FromRowError};
use crate::frame::{
    response::{
        result,
        result::{Row, Rows},
        Response,
    },
    value::SerializedValues,
};
use crate::routing::Token;
use crate::statement::Consistency;
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::cluster::ClusterData;
use crate::transport::connection::{Connection, QueryResponse};
use crate::transport::load_balancing::{LoadBalancingPolicy, Statement};
use crate::transport::metrics::Metrics;
use crate::transport::node::Node;
use crate::transport::retry_policy::{QueryInfo, RetryDecision, RetryPolicy, RetrySession};

pub struct RowIterator {
    current_row_idx: usize,
    current_page: Rows,
    page_receiver: mpsc::Receiver<Result<ReceivedPage, QueryError>>,
    tracing_ids: Vec<Uuid>,
}

struct ReceivedPage {
    pub rows: Rows,
    pub tracing_id: Option<Uuid>,
}

impl Stream for RowIterator {
    type Item = Result<Row, QueryError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        if s.is_current_page_exhausted() {
            match Pin::new(&mut s.page_receiver).poll_recv(cx) {
                Poll::Ready(Some(Ok(received_page))) => {
                    s.current_page = received_page.rows;
                    s.current_row_idx = 0;

                    if let Some(tracing_id) = received_page.tracing_id {
                        s.tracing_ids.push(tracing_id);
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        let idx = s.current_row_idx;
        if idx < s.current_page.rows.len() {
            let row = mem::take(&mut s.current_page.rows[idx]);
            s.current_row_idx += 1;
            return Poll::Ready(Some(Ok(row)));
        }

        // We probably got a zero-sized page
        // Yield, but tell that we are ready
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RowIterator {
    pub fn into_typed<RowT: FromRow>(self) -> TypedRowIterator<RowT> {
        TypedRowIterator {
            row_iterator: self,
            phantom_data: Default::default(),
        }
    }

    pub(crate) fn new_for_query(
        query: Query,
        values: SerializedValues,
        retry_policy: Box<dyn RetryPolicy>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
        mut speculative_execution_policy: Option<SpeculativeExecutionPolicy>,
    ) -> RowIterator {
        let (sender, receiver) = mpsc::channel(1);

        if !query.config.is_idempotent {
            speculative_execution_policy = None;
        }

        tokio::task::spawn(Self::query_task(
            query,
            values,
            retry_policy,
            load_balancer,
            cluster_data,
            metrics,
            speculative_execution_policy,
            sender,
        ));

        RowIterator {
            current_row_idx: 0,
            current_page: Default::default(),
            page_receiver: receiver,
            tracing_ids: Vec::new(),
        }
    }

    async fn query_task(
        query: Query,
        values: SerializedValues,
        retry_policy: Box<dyn RetryPolicy>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
        speculative_execution_policy: Option<SpeculativeExecutionPolicy>,
        sender: mpsc::Sender<Result<ReceivedPage, QueryError>>,
    ) {
        let statement_info = Default::default();

        let query_plan = load_balancer.plan(&statement_info, &cluster_data);
        let shared_plan = SharedPlan {
            iter: Arc::new(std::sync::Mutex::new(query_plan)),
        };

        let worker_generator = |speculative: Option<SpeculativeAborter>| {
            let query_ref = &query;
            let values_ref = &values;

            let choose_connection = |node: Arc<Node>| async move { node.random_connection().await };

            let page_query = move |connection: Arc<Connection>, paging_state: Option<Bytes>| async move {
                connection.query(query_ref, values_ref, paging_state).await
            };

            let worker = RowIteratorWorker {
                sender: sender.clone(),
                aborter: speculative,
                choose_connection,
                page_query,
                query_is_idempotent: query.config.is_idempotent,
                query_consistency: query.config.consistency,
                retry_session: retry_policy.new_session(),
                metrics: metrics.clone(),
                paging_state: None,
            };

            worker.work(&shared_plan)
        };

        Self::run_worker(worker_generator, speculative_execution_policy).await;
    }

    pub(crate) fn new_for_prepared_statement(
        prepared: PreparedStatement,
        values: SerializedValues,
        token: Token,
        retry_policy: Box<dyn RetryPolicy>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
        mut speculative_execution_policy: Option<SpeculativeExecutionPolicy>,
    ) -> RowIterator {
        let (sender, receiver) = mpsc::channel(1);

        if !prepared.config.is_idempotent {
            speculative_execution_policy = None;
        }

        tokio::task::spawn(Self::prepared_statement_task(
            prepared,
            values,
            token,
            retry_policy,
            load_balancer,
            cluster_data,
            metrics,
            speculative_execution_policy,
            sender,
        ));

        RowIterator {
            current_row_idx: 0,
            current_page: Default::default(),
            page_receiver: receiver,
            tracing_ids: Vec::new(),
        }
    }

    async fn prepared_statement_task(
        prepared: PreparedStatement,
        values: SerializedValues,
        token: Token,
        retry_policy: Box<dyn RetryPolicy>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        cluster_data: Arc<ClusterData>,
        metrics: Arc<Metrics>,
        speculative_execution_policy: Option<SpeculativeExecutionPolicy>,
        sender: mpsc::Sender<Result<ReceivedPage, QueryError>>,
    ) {
        let statement_info = Statement {
            token: Some(token),
            keyspace: None,
        };

        let query_plan = load_balancer.plan(&statement_info, &cluster_data);
        let shared_plan = SharedPlan {
            iter: Arc::new(std::sync::Mutex::new(query_plan)),
        };

        let worker_generator = |speculative: Option<SpeculativeAborter>| {
            let prepared_ref = &prepared;
            let values_ref = &values;

            let choose_connection =
                |node: Arc<Node>| async move { node.connection_for_token(token).await };

            let page_query = move |connection: Arc<Connection>, paging_state: Option<Bytes>| async move {
                connection
                    .execute(prepared_ref, values_ref, paging_state)
                    .await
            };

            let worker = RowIteratorWorker {
                sender: sender.clone(),
                aborter: speculative,
                choose_connection,
                page_query,
                query_is_idempotent: prepared.config.is_idempotent,
                query_consistency: prepared.config.consistency,
                retry_session: retry_policy.new_session(),
                metrics: metrics.clone(),
                paging_state: None,
            };

            worker.work(&shared_plan)
        };

        Self::run_worker(worker_generator, speculative_execution_policy).await;
    }

    async fn run_worker<F, I>(
        worker_generator: impl Fn(Option<SpeculativeAborter>) -> F,
        policy: Option<SpeculativeExecutionPolicy>,
    ) where
        F: Future<Output = Option<I>>,
    {
        match policy {
            None => {
                worker_generator(None).await;
            }
            Some(policy) => {
                let executor = SpeculativeExecutor { policy };
                executor.execute(worker_generator).await;
            }
        }
    }

    /// Returns tracing ids of all finished queries that had tracing enabled
    pub fn get_tracing_ids(&self) -> &[Uuid] {
        &self.tracing_ids
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_row_idx >= self.current_page.rows.len()
    }
}

// RowIteratorWorker works in the background to fetch pages
// RowIterator receives them through a channel
struct RowIteratorWorker<ConnFunc, QueryFunc> {
    sender: mpsc::Sender<Result<ReceivedPage, QueryError>>,

    // Used to stop other workers after receiving first page in speculative execution mode
    aborter: Option<SpeculativeAborter>,

    // Closure used to choose a connection from a node
    // AsyncFn(Arc<Node>) -> Result<Arc<Connection>, QueryError>
    choose_connection: ConnFunc,

    // Closure used to perform a single page query
    // AsyncFn(Arc<Connection>, Option<Bytes>) -> Result<QueryResponse, QueryError>
    page_query: QueryFunc,

    query_is_idempotent: bool,
    query_consistency: Consistency,

    retry_session: Box<dyn RetrySession>,
    //speculative_execution: Option<SpeculativeExecutionPolicy>,
    metrics: Arc<Metrics>,

    paging_state: Option<Bytes>,
}

impl<ConnFunc, ConnFut, QueryFunc, QueryFut> RowIteratorWorker<ConnFunc, QueryFunc>
where
    ConnFunc: Fn(Arc<Node>) -> ConnFut,
    ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
    QueryFunc: Fn(Arc<Connection>, Option<Bytes>) -> QueryFut,
    QueryFut: Future<Output = Result<QueryResponse, QueryError>>,
{
    async fn work(mut self, query_plan: impl Iterator<Item = Arc<Node>>) -> Option<()> {
        let mut last_error: Option<QueryError> = None;

        'nodes_in_plan: for node in query_plan {
            // For each node in the plan choose a connection to use
            // This connection will be reused for same node retries to preserve paging cache on the shard
            let connection: Arc<Connection> = match (self.choose_connection)(node).await {
                Ok(connection) => connection,
                Err(e) => {
                    last_error = Some(e);
                    // Broken connection doesn't count as a failed query, don't log in metrics
                    continue 'nodes_in_plan;
                }
            };

            'same_node_retries: loop {
                // Query pages until an error occurs
                let queries_result: Result<(), QueryError> = self.query_pages(&connection).await;

                last_error = match queries_result {
                    Ok(()) => return Some(()),
                    Err(error) => Some(error),
                };

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: last_error.as_ref().unwrap(),
                    is_idempotent: self.query_is_idempotent,
                    consistency: self.query_consistency,
                };

                match self.retry_session.decide_should_retry(query_info) {
                    RetryDecision::RetrySameNode => {
                        self.metrics.inc_retries_num();
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextNode => {
                        self.metrics.inc_retries_num();
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,
                };
            }
        }

        if let Some(err) = last_error {
            // Stop other fibers if executing speculatively
            if let Some(s) = self.aborter.take() {
                s.abort_others();
            }

            // Send last_error to RowIterator - query failed fully
            let _ = self.sender.send(Err(err)).await;
            return Some(());
        }

        return None;
    }

    // Given a working connection query as many pages as possible until the first error
    async fn query_pages(&mut self, connection: &Arc<Connection>) -> Result<(), QueryError> {
        loop {
            self.metrics.inc_total_paged_queries();
            let query_start = std::time::Instant::now();

            let query_response: QueryResponse =
                (self.page_query)(connection.clone(), self.paging_state.clone()).await?;

            match query_response.response {
                Response::Result(result::Result::Rows(mut rows)) => {
                    let _ = self
                        .metrics
                        .log_query_latency(query_start.elapsed().as_millis() as u64);

                    self.paging_state = rows.metadata.paging_state.take();

                    let received_page = ReceivedPage {
                        rows,
                        tracing_id: query_response.tracing_id,
                    };

                    // If we got to this place, and are still executing speculatively, then it's
                    // time to stop other fibers
                    if let Some(s) = self.aborter.take() {
                        s.abort_others();
                    }

                    // Send next page to RowIterator
                    if self.sender.send(Ok(received_page)).await.is_err() {
                        // channel was closed, RowIterator was dropped - should shutdown
                        return Ok(());
                    }

                    if self.paging_state.is_none() {
                        // Reached the last query, shutdown
                        return Ok(());
                    }

                    // Query succeded, reset retry policy for future retries
                    self.retry_session.reset();
                }
                Response::Error(err) => {
                    self.metrics.inc_failed_paged_queries();
                    return Err(err.into());
                }
                _ => {
                    self.metrics.inc_failed_paged_queries();

                    return Err(QueryError::ProtocolError(
                        "Unexpected response to next page query",
                    ));
                }
            }
        }
    }
}

pub struct TypedRowIterator<RowT> {
    row_iterator: RowIterator,
    phantom_data: std::marker::PhantomData<RowT>,
}

impl<RowT> TypedRowIterator<RowT> {
    /// Returns tracing ids of all finished queries that had tracing enabled
    pub fn get_tracing_ids(&self) -> &[Uuid] {
        self.row_iterator.get_tracing_ids()
    }
}

/// Couldn't get next typed row from the iterator
#[derive(Error, Debug, Clone)]
pub enum NextRowError {
    /// Query to fetch next page has failed
    #[error(transparent)]
    QueryError(#[from] QueryError),

    /// Parsing values in row as given types failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

impl<RowT: FromRow> Stream for TypedRowIterator<RowT> {
    type Item = Result<RowT, NextRowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        let next_elem: Option<Result<Row, QueryError>> =
            match Pin::new(&mut s.row_iterator).poll_next(cx) {
                Poll::Ready(next_elem) => next_elem,
                Poll::Pending => return Poll::Pending,
            };

        let next_ready: Option<Self::Item> = match next_elem {
            Some(Ok(next_row)) => Some(RowT::from_row(next_row).map_err(|e| e.into())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        };

        Poll::Ready(next_ready)
    }
}

// TypedRowIterator can be moved freely for any RowT so it's Unpin
impl<RowT> Unpin for TypedRowIterator<RowT> {}

// TODO delete this
struct SharedPlan<I>
where
    I: Iterator<Item = Arc<Node>>,
{
    iter: Arc<std::sync::Mutex<I>>,
}

impl<I> Iterator for &SharedPlan<I>
where
    I: Iterator<Item = Arc<Node>>,
{
    type Item = Arc<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.lock().unwrap().next()
    }
}

struct AbortHandleSet {
    handles: Vec<AbortHandle>,
    was_aborted: bool,
}

impl AbortHandleSet {
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            handles: Vec::new(),
            was_aborted: false,
        }))
    }

    fn abort_others(&mut self, id: usize) {
        self.was_aborted = true;

        let iter = self
            .handles
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != id)
            .map(|(_, h)| h);

        for handle in iter {
            handle.abort();
        }
    }

    // wraps future with Abortable, and registers AbortHandle
    fn register<I>(
        &mut self,
        future: impl Future<Output = Option<I>>,
    ) -> impl Future<Output = Option<I>> {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let future = Abortable::new(future, abort_registration);

        self.handles.push(abort_handle);

        future.map(|r| r.ok().flatten())
    }
}

struct SpeculativeAborter {
    worker_id: usize,
    aborter: Arc<Mutex<AbortHandleSet>>,
}

impl SpeculativeAborter {
    fn abort_others(&self) {
        self.aborter.lock().unwrap().abort_others(self.worker_id);
    }
}

struct SpeculativeExecutor {
    policy: SpeculativeExecutionPolicy,
}

impl SpeculativeExecutor {
    pub async fn execute<F, I>(&self, generator: impl Fn(Option<SpeculativeAborter>) -> F)
    where
        F: Future<Output = Option<I>>,
    {
        let aborter = AbortHandleSet::new();

        let mut tasks = (0..self.policy.max_retry_count + 1)
            .map(|i| SpeculativeAborter {
                worker_id: i,
                aborter: aborter.clone(),
            })
            .map(|worker| aborter.lock().unwrap().register(generator(Some(worker))));

        let mut async_tasks = FuturesUnordered::new();
        async_tasks.push(tasks.next().unwrap());

        let sleep = tokio::time::sleep(self.policy.retry_interval).fuse();
        tokio::pin!(sleep);

        loop {
            futures::select! {
                _ = &mut sleep => {
                    if !aborter.lock().unwrap().was_aborted {
                        let task = match tasks.next() {
                            Some(t) => t,
                            None => continue,
                        };

                        async_tasks.push(task);

                        // reset the timeout
                        sleep.set(tokio::time::sleep(self.policy.retry_interval).fuse());
                    }
                }
                res = async_tasks.select_next_some() => {
                    if res.is_some() {
                        return
                    }
                }
                complete => break
            }
        }
    }
}
