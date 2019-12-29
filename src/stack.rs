use crate::make_reader;
use crate::op_variants;
use crate::types::{Key, ReturnValue, StateRef, Value};

op_variants! {
    StackOps,
    STPush(Key, Value),
    STPop(Key),
    STPeek(Key),
    STSize(Key)
}

make_reader!(stacks, read_stacks);

pub async fn stack_interact(stack_op: StackOps, state: StateRef) -> ReturnValue {
    match stack_op {
        StackOps::STPush(key, value) => {
            let mut stack_lock = state.stacks.write();
            stack_lock.entry(key).or_default().push(value).into()
        }
        StackOps::STPop(key) => {
            let mut stack_lock = state.stacks.write();
            stack_lock
                .get_mut(&key)
                .and_then(|st| st.pop())
                .map(ReturnValue::StringRes)
                .unwrap_or(ReturnValue::Nil)
        }
        StackOps::STPeek(key) => read_stacks!(state)
            .get(&key)
            .and_then(|st| st.peek())
            .map(ReturnValue::StringRes)
            .unwrap_or(ReturnValue::Nil),
        StackOps::STSize(key) => read_stacks!(state)
            .get(&key)
            .map(|st| st.size())
            .map(ReturnValue::IntRes)
            .unwrap_or(ReturnValue::Nil),
    }
}
