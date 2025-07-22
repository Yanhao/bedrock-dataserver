//! GroupCommitter module
//! Implements batch request aggregation and async processing to improve WAL write throughput and reduce latency.
//! Supports custom batch size, commit interval, and is thread-safe.

use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use derivative::Derivative;
use futures_util::Future;
use tracing::error;
use typed_builder::TypedBuilder;

use crate::utils::R;

/// Struct for request and notifier
/// arg: request argument
/// notifier: oneshot channel for async result notification
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RequestAndNotifier<Arg, Ret> {
    arg: Arg,
    #[derivative(Debug = "ignore")]
    notifier: Option<tokio::sync::oneshot::Sender<(bool, Ret)>>,
}

/// GroupCommitter
/// - input_channel_length: input channel length
/// - reap_group_size: max batch size per group
/// - reap_interval: max aggregation delay
/// - input: input channel
/// - extra: extra parameter
/// - stop_ch: stop signal
#[derive(TypedBuilder)]
pub struct GroupCommitter<Arg, Ret, Extra> {
    #[builder(default = 10240)]
    input_channel_length: usize,
    #[builder(default = 20)]
    reap_group_size: usize,
    #[builder(default_code = r#"chrono::Duration::milliseconds(5)"#)]
    reap_interval: chrono::Duration,

    #[builder(default)]
    #[allow(clippy::type_complexity)]
    input: ArcSwapOption<tokio::sync::mpsc::Sender<RequestAndNotifier<Arg, Ret>>>,

    #[builder(default)]
    extra: Option<Extra>,

    #[builder(default = None)]
    stop_ch: Option<tokio::sync::watch::Sender<()>>,
}

impl<Arg, Ret, Extra> GroupCommitter<Arg, Ret, Extra>
where
    Ret: Sync + Send + Clone + 'static,
    Arg: Sync + Send + Clone + 'static,
    Extra: Sync + Send + Clone + 'static,
{
    /// Submit a single request, returns (is last in batch, result)
    pub async fn r#do(&self, arg: Arg) -> Result<(bool, Ret)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req_with_notifier = RequestAndNotifier {
            arg,
            notifier: Some(tx),
        };

        self.input.load().r().send(req_with_notifier).await?;

        Ok(rx.await?)
    }

    /// Aggregate batch requests and call func to process, notify result via notifier
    async fn try_reap_request<Func, FuncRet>(
        mut request: Vec<RequestAndNotifier<Arg, Ret>>,
        func: Func,
        extra: Option<Extra>,
    ) where
        Func: Fn(Option<Extra>, Vec<Arg>) -> FuncRet + Send + Sync + Clone + 'static,
        FuncRet: Future<Output = Ret> + Send,
    {
        let args: Vec<_> = request.iter().map(|x| x.arg.clone()).collect();

        let res = func(extra, args).await;

        if let Some(mut tail) = request.pop() {
            match tail.notifier.take() {
                Some(notifier) => {
                    let _ = notifier.send((true, res.clone()));
                }
                None => {
                    error!("GroupCommitter: tail notifier is None (should always be Some)");
                }
            }
        }

        for mut req in request.into_iter() {
            match req.notifier.take() {
                Some(notifier) => {
                    let _ = notifier.send((false, res.clone()));
                }
                None => {
                    error!("GroupCommitter: batch notifier is None (should always be Some)");
                }
            }
        }
    }

    /// Start group commit background task, triggers func by timer or batch size
    pub fn start<Func, FuncRet>(&mut self, func: Func)
    where
        Func: Fn(Option<Extra>, Vec<Arg>) -> FuncRet + Send + Sync + Clone + 'static,
        FuncRet: Future<Output = Ret> + Send,
    {
        if self.stop_ch.is_some() {
            return;
        }

        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel::<()>(());
        self.stop_ch.replace(stop_tx);

        let (input_tx, mut input_rx) =
            tokio::sync::mpsc::channel::<RequestAndNotifier<Arg, Ret>>(self.input_channel_length);

        self.input.store(Some(Arc::new(input_tx)));

        let reap_batch_size = self.reap_group_size;
        let reap_interval = self.reap_interval;
        let extra = self.extra.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reap_interval.clone().to_std().unwrap());
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut batch = Vec::with_capacity(reap_batch_size);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if !batch.is_empty() {
                            let mut batch_c = Vec::with_capacity(reap_batch_size);
                            std::mem::swap(&mut batch, &mut batch_c);

                            let func = func.clone();
                            let extra = extra.clone();
                            tokio::spawn(async move {
                                Self::try_reap_request(batch_c, func, extra).await;
                            });
                        }
                    }

                    req = input_rx.recv() => {
                        if let Some(req) = req {
                            batch.push(req);
                        } else {
                            error!("input receive None");
                        }

                        if batch.len() >= reap_batch_size {
                            let mut batch_c = Vec::with_capacity(reap_batch_size);
                            std::mem::swap(&mut batch, &mut batch_c);
                            ticker.reset();

                            let func = func.clone();
                            let extra = extra.clone();
                            tokio::spawn(async move {
                                Self::try_reap_request(batch_c, func, extra).await;
                            });
                        }
                    }

                    _ = stop_rx.changed() => {
                        return;
                    }
                }
            }
        });
    }
}
