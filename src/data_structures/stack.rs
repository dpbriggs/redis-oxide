use crate::types::Count;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
pub struct Stack<T> {
    inner: Vec<T>,
}

impl<T: Clone> Stack<T> {
    pub fn new() -> Stack<T> {
        Stack { inner: Vec::new() }
    }

    pub fn push(&mut self, item: T) -> Count {
        self.inner.push(item);
        self.inner.len() as Count
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop()
    }

    pub fn peek(&self) -> Option<T> {
        self.inner.last().cloned()
    }

    pub fn size(&self) -> Count {
        self.inner.len() as Count
    }
}

#[cfg(test)]
mod test_stack {
    use crate::data_structures::stack::Stack;

    #[test]
    fn test_push_pop() {
        let mut s = Stack::new();
        s.push(3);
        assert_eq!(s.pop(), Some(3));
        assert_eq!(s.pop(), None);
    }

    #[test]
    fn test_peek_size() {
        let mut s = Stack::new();
        assert_eq!(s.size(), 0);
        assert_eq!(s.peek(), None);
        s.push(3);
        assert_eq!(s.peek(), Some(3));
        assert_eq!(s.peek(), Some(3));
        assert_eq!(s.size(), 1);
        s.push(4);
        assert_eq!(s.peek(), Some(4));
        assert_eq!(s.size(), 2);
    }
}
