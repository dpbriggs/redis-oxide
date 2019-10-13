use std::collections::HashMap;

type Receipt = u32;

#[derive(Default, Debug)]
struct RecieptMap<T> {
    counter: Receipt,
    holder: HashMap<Receipt, T>,
}

impl<T: Default> RecieptMap<T> {
    fn new() -> RecieptMap<T> {
        Default::default()
    }

    fn insert(&mut self, item: T) -> Receipt {
        self.counter += 1;
        self.holder.insert(self.counter, item);
        self.counter
    }

    fn return_receipt(&mut self, receipt: Receipt) -> Option<T> {
        self.holder.remove(&receipt)
    }
}

struct KeyWake {
    state: u32,
}
