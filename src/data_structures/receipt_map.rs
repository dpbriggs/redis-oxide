use seahash::hash;
use std::collections::HashMap;
use std::collections::HashSet;
use std::task::Waker;

pub type Receipt = u32;

#[derive(Hash, Debug, PartialEq, Eq)]
pub enum KeyTypes {
    List(u64),
}

impl KeyTypes {
    pub fn list(key: &[u8]) -> KeyTypes {
        KeyTypes::List(hash(key))
    }
}

#[derive(Default, Debug)]
pub struct RecieptMap {
    counter: Receipt,
    wakers: HashMap<Receipt, Waker>,
    timed_out: HashSet<Receipt>,
    keys: HashMap<KeyTypes, Vec<Receipt>>,
}

impl RecieptMap {
    pub fn get_receipt(&mut self) -> Receipt {
        self.counter += 1;
        self.counter
    }

    pub fn insert(&mut self, receipt: Receipt, item: Waker, key: KeyTypes) {
        self.wakers.insert(receipt, item);
        self.keys.entry(key).or_default().push(receipt);
    }

    pub fn receipt_timed_out(&self, receipt: Receipt) -> bool {
        self.timed_out.contains(&receipt)
    }

    pub fn wake_with_key(&mut self, key: KeyTypes) {
        let v = self.keys.get_mut(&key);
        if v.is_none() {
            return;
        }
        let v = v.unwrap();
        while let Some(receipt) = v.pop() {
            match self.wakers.remove(&receipt) {
                Some(waker) => {
                    waker.wake();
                    break;
                }
                None => continue,
            };
        }
    }

    pub fn timeout_receipt(&mut self, receipt: Receipt) {
        self.timed_out.insert(receipt);
        if let Some(waker) = self.wakers.remove(&receipt) {
            waker.wake();
        }
    }
}
