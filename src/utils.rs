pub(crate) trait R<T> {
    fn r(&self) -> &T;
}

impl<T> R<T> for Option<T> {
    fn r(&self) -> &T {
        self.as_ref().unwrap()
    }
}

pub(crate) trait A<T> {
    fn s(&self, val: T);
}

impl<T> A<T> for arc_swap::ArcSwapOption<T> {
    #[inline]
    fn s(&self, val: T) {
        drop(self.swap(Some(std::sync::Arc::new(val))));
    }
}
