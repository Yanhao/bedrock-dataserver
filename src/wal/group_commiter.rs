use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use derivative::Derivative;
use futures_util::Future;
use tracing::error;
use typed_builder::TypedBuilder;

use crate::utils::R;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RequestAndNotifier<Arg, Ret> {
    arg: Arg,
    #[derivative(Debug = "ignore")]
    notifier: Option<tokio::sync::oneshot::Sender<(bool, Ret)>>,
}

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
    pub async fn r#do(&self, arg: Arg) -> Result<(bool, Ret)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req_with_notifier = RequestAndNotifier {
            arg,
            notifier: Some(tx),
        };

        self.input.load().r().send(req_with_notifier).await?;

        Ok(rx.await?)
    }

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
            let _ = tail.notifier.take().unwrap().send((true, res.clone()));
        }

        for mut req in request.into_iter() {
            let _ = req.notifier.take().unwrap().send((false, res.clone()));
        }
    }

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
