use crate::{
    blockcfg::{Block, HeaderHash},
    start_up::{NodeStorage, NodeStorageConnection},
};
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use chain_storage::store::{for_path_to_nth_ancestor, BlockInfo, BlockStore};
use std::sync::Arc;
use tokio::prelude::future::Either;
use tokio::prelude::*;
use tokio::sync::lock::Lock;
use tokio_02::{sync::Mutex, task::spawn_blocking};
use tokio_compat::prelude::*;

pub use chain_storage::error::Error as StorageError;

#[derive(Clone)]
struct ConnectionManager {
    inner: Arc<NodeStorage>,
}

impl ConnectionManager {
    pub fn new(storage: NodeStorage) -> Self {
        Self {
            inner: Arc::new(storage),
        }
    }
}

#[async_trait]
impl ManageConnection for ConnectionManager {
    type Connection = NodeStorageConnection;
    type Error = StorageError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let inner = self.inner.clone();
        spawn_blocking(move || inner.connect())
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)))
            .and_then(std::convert::identity)
    }

    async fn is_valid(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        spawn_blocking(move || conn.ping().and(Ok(conn)))
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)))
            .and_then(std::convert::identity)
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.ping().is_ok()
    }
}

#[derive(Clone)]
pub struct Storage {
    pool: Pool<ConnectionManager>,

    // All write operations must be performed only via this lock. The lock helps
    // us to ensure that all of the write operations are performed in the right
    // sequence. Otherwise they can be performed out of the expected order (for
    // example, by different tokio executors) which eventually leads to a panic
    // because the block data would be inconsistent at the time of a write.
    write_connection_lock: Arc<Mutex<NodeStorageConnection>>,
}

pub struct BlockStream<'a> {
    pool: Pool<ConnectionManager>,
    inner: PooledConnection<'a, ConnectionManager>,
    state: BlockIterState,
}

pub struct Ancestor {
    pub header_hash: HeaderHash,
    pub distance: u64,
}

struct BlockIterState {
    to_depth: u64,
    cur_depth: u64,
    pending_infos: Vec<BlockInfo<HeaderHash>>,
}

// async fn blocking_storage_op()

impl Storage {
    pub async fn new(storage: NodeStorage) -> Self {
        let manager = ConnectionManager::new(storage);
        let pool = Pool::builder().build(manager).await.unwrap();
        let write_connection_lock =
            Arc::new(Mutex::new(pool.dedicated_connection().await.unwrap()));

        Storage {
            pool,
            write_connection_lock,
        }
    }

    async fn run<F, R>(&self, f: F) -> Result<R, StorageError>
    where
        F: FnOnce(PooledConnection<'_, ConnectionManager>) -> Result<R, StorageError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let connection = self
            .pool
            .clone()
            .get()
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)))?;

        spawn_blocking(move || f(connection))
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)))
            .and_then(std::convert::identity)
    }

    pub async fn get_tag(&self, tag: String) -> Result<Option<HeaderHash>, StorageError> {
        self.run(|connection| connection.get_tag(&tag)).await
    }

    pub async fn put_tag(&self, tag: String, header_hash: HeaderHash) -> Result<(), StorageError> {
        let guard = self.write_connection_lock.clone().lock().await;
        spawn_blocking(move || guard.put_tag(&tag, &header_hash))
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)))
            .and_then(std::convert::identity)
    }

    pub async fn get(&self, header_hash: HeaderHash) -> Result<Option<Block>, StorageError> {
        self.run(|connection| match connection.get_block(&header_hash) {
            Err(StorageError::BlockNotFound) => Ok(None),
            Ok((block, _block_info)) => Ok(Some(block)),
            Err(e) => Err(e),
        })
        .await
    }

    pub async fn get_with_info(
        &self,
        header_hash: HeaderHash,
    ) -> Result<Option<(Block, BlockInfo<HeaderHash>)>, StorageError> {
        self.run(|connection| match connection.get_block(&header_hash) {
            Err(StorageError::BlockNotFound) => Ok(None),
            Ok(v) => Ok(Some(v)),
            Err(e) => Err(e),
        })
        .await
    }

    pub async fn block_exists(&self, header_hash: HeaderHash) -> Result<bool, StorageError> {
        self.run(|connection| match connection.block_exists(&header_hash) {
            Err(StorageError::BlockNotFound) => Ok(false),
            Ok(r) => Ok(r),
            Err(e) => Err(e),
        })
        .await
    }

    pub async fn put_block(&self, block: Block) -> Result<(), StorageError> {
        let guard = self.write_connection_lock.clone().lock().await;
        spawn_blocking(move || match guard.put_block(&block) {
            Err(StorageError::BlockNotFound) => unreachable!(),
            Err(e) => Err(e),
            Ok(()) => Ok(()),
        })
        .await
        .map_err(|e| StorageError::BackendError(Box::new(e)))
        .and_then(std::convert::identity)
    }

    /// Return values:
    /// - `Ok(stream)` - `from` is ancestor of `to`, returns blocks between them
    /// - `Err(CannotIterate)` - `from` is not ancestor of `to`
    /// - `Err(BlockNotFound)` - `from` or `to` was not found
    /// - `Err(_)` - some other storage error
    pub async fn stream_from_to(
        &self,
        from: HeaderHash,
        to: HeaderHash,
    ) -> Result<BlockStream<'_>, StorageError> {
        let pool = self.pool.clone();

        let connection = pool
            .get()
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)))?;

        spawn_blocking(move || match connection.is_ancestor(&from, &to) {
            Ok(Some(distance)) => match connection.get_block_info(&to) {
                Ok(to_info) => Ok(BlockStream {
                    pool,
                    inner: connection,
                    state: BlockIterState::new(to_info, distance),
                }),
                Err(e) => Err(e),
            },
            Ok(None) => Err(StorageError::CannotIterate),
            Err(e) => Err(e),
        })
        .await
        .map_err(|e| StorageError::BackendError(Box::new(e)))
        .and_then(std::convert::identity)
    }

    /// Stream a branch ending at `to` and starting from the ancestor
    /// at `depth` or at the first ancestor since genesis block
    /// if `depth` is given as `None`.
    ///
    /// This function uses buffering in the sink to reduce lock contention.
    pub async fn send_branch<S, E>(
        &self,
        to: HeaderHash,
        depth: Option<u64>,
        sink: S,
    ) -> Result<(), S::SinkError>
    where
        S: Sink<SinkItem = Result<Block, E>>,
        E: From<StorageError>,
    {
        let connection_res = self
            .pool
            .clone()
            .get()
            .await
            .map_err(|e| StorageError::BackendError(Box::new(e)));

        let res = match connection_res {
            Ok(connection) => {
                let iter_res = spawn_blocking(move || {
                    connection.get_block_info(&to).map(|to_info| {
                        let depth = depth.unwrap_or(to_info.depth - 1);
                        BlockIterState::new(to_info, depth)
                    })
                })
                .await
                .map_err(|e| StorageError::BackendError(Box::new(e)))
                .and_then(std::convert::identity);
                match iter_res {
                    Ok(iter) => Ok((iter, connection)),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };

        match res {
            Ok((iter, connection)) => {
                let mut state = SendState {
                    sink,
                    iter,
                    pending: None,
                };
                let fut = future::poll_fn(move || {
                    while try_ready!(state.poll_continue()) {
                        try_ready!(state.fill_sink(&connection));
                    }
                    Ok(().into())
                });
                Either::A(fut)
            }
            Err(e) => {
                let fut = sink
                    .send_all(stream::once(Ok(Err(e.into()))))
                    .map(|(_, _)| ());
                Either::B(fut)
            }
        }
        .compat()
        .await
    }

    pub async fn find_closest_ancestor(
        &self,
        checkpoints: Vec<HeaderHash>,
        descendant: HeaderHash,
    ) -> Result<Option<Ancestor>, StorageError> {
        self.run(|connection| {
            let mut ancestor = None;
            let mut closest_found = std::u64::MAX;
            for checkpoint in checkpoints {
                // Checkpoints sent by a peer may not
                // be present locally, so we need to ignore certain errors
                match connection.is_ancestor(&checkpoint, &descendant) {
                    Ok(None) => {}
                    Ok(Some(distance)) => {
                        if closest_found > distance {
                            ancestor = Some(checkpoint);
                            closest_found = distance;
                        }
                    }
                    Err(e) => {
                        // Checkpoints sent by a peer may not
                        // be present locally, so we need to ignore certain errors
                        match e {
                            StorageError::BlockNotFound => {
                                // FIXME: add block hash into the error so we
                                // can see which of the two it is.
                                // For now, just ignore either.
                            }
                            _ => return Err(e),
                        }
                    }
                }
            }
            Ok(ancestor.map(|header_hash| Ancestor {
                header_hash,
                distance: closest_found,
            }))
        })
        .await
    }
}

impl<'a> Stream for BlockStream<'a> {
    type Item = Block;
    type Error = StorageError;

    fn poll(&mut self) -> Poll<Option<Block>, Self::Error> {
        if !self.state.has_next() {
            return Ok(Async::Ready(None));
        }

        self.state
            .get_next(&mut self.inner)
            .map(|block| Async::Ready(Some(block)))
    }
}

impl BlockIterState {
    fn new(to_info: BlockInfo<HeaderHash>, distance: u64) -> Self {
        BlockIterState {
            to_depth: to_info.depth,
            cur_depth: to_info.depth - distance,
            pending_infos: vec![to_info],
        }
    }

    fn has_next(&self) -> bool {
        self.cur_depth < self.to_depth
    }

    fn get_next(&mut self, store: &NodeStorageConnection) -> Result<Block, StorageError> {
        assert!(self.has_next());

        self.cur_depth += 1;

        let block_info = self.pending_infos.pop().unwrap();

        if block_info.depth == self.cur_depth {
            // We've seen this block on a previous ancestor traversal.
            let (block, _block_info) = store.get_block(&block_info.block_hash)?;
            Ok(block)
        } else {
            // We don't have this block yet, so search back from
            // the furthest block that we do have.
            assert!(self.cur_depth < block_info.depth);
            let depth = block_info.depth;
            let parent = block_info.parent_id();
            self.pending_infos.push(block_info);
            let block_info = for_path_to_nth_ancestor(
                &*store,
                &parent,
                depth - self.cur_depth - 1,
                |new_info| {
                    self.pending_infos.push(new_info.clone());
                },
            )?;

            let (block, _block_info) = store.get_block(&block_info.block_hash)?;
            Ok(block)
        }
    }
}

struct SendState<S, E> {
    sink: S,
    iter: BlockIterState,
    pending: Option<Result<Block, E>>,
}

impl<S, E> SendState<S, E>
where
    S: Sink<SinkItem = Result<Block, E>>,
    E: From<StorageError>,
{
    fn poll_continue(&mut self) -> Poll<bool, S::SinkError> {
        if let Some(item) = self.pending.take() {
            match self.sink.start_send(item)? {
                AsyncSink::Ready => {}
                AsyncSink::NotReady(item) => {
                    self.pending = Some(item);
                    return Ok(Async::NotReady);
                }
            }
        }

        let has_next = self.iter.has_next();

        if has_next {
            // Flush the sink before locking to send more blocks
            try_ready!(self.sink.poll_complete());
        } else {
            try_ready!(self.sink.close());
        }

        Ok(has_next.into())
    }

    fn fill_sink(&mut self, store: &NodeStorageConnection) -> Poll<(), S::SinkError> {
        assert!(self.iter.has_next());
        loop {
            let item = self.iter.get_next(store).map_err(Into::into);
            match self.sink.start_send(item)? {
                AsyncSink::Ready => {
                    if !self.iter.has_next() {
                        return Ok(().into());
                    } else {
                        // FIXME: have to yield and release the storage lock
                        // because .get_next() may block on database access,
                        // starving other storage access queries.
                        // https://github.com/input-output-hk/jormungandr/issues/1263
                        task::current().notify();
                        return Ok(Async::NotReady);
                    }
                }
                AsyncSink::NotReady(item) => {
                    self.pending = Some(item);
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}
