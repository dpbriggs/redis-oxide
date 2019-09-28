use crate::types::{Count, Index, InteractionRes, Key, ReturnValue, Score, State, StateInteration};
use crate::{make_reader, make_writer};

/// ZSet Ops
#[derive(Clone, Debug)]
pub enum ZSetOps {
    ZAdd(Key, Vec<(Score, Key)>),
    ZRem(Key, Vec<Key>),
    ZRange(Key, Score, Score),
    ZCard(Key),
    ZScore(Key, Key),
    ZPopMax(Key, Count),
    ZPopMin(Key, Count),
    ZRank(Key, Key),
}

make_reader!(zsets, read_zsets);
make_writer!(zsets, write_zsets);

fn deal_with_negative_indices(coll_size: Count, bounds: (Index, Index)) -> (Index, Index) {
    let (start, end) = bounds;
    let start = if start < 0 { start + coll_size } else { start };
    let end = if end < 0 { end + coll_size } else { end };
    (start, end)
}

impl StateInteration for ZSetOps {
    fn interact(self, state: State) -> InteractionRes {
        match self {
            ZSetOps::ZAdd(zset_key, member_scores) => {
                state.create_zset_if_necessary(&zset_key);
                write_zsets!(state, &zset_key, zset);
                let num_added = zset.add(member_scores);
                ReturnValue::IntRes(num_added).into()
            }
            ZSetOps::ZRem(zset_key, keys) => write_zsets!(state, &zset_key)
                .map(|zset| zset.remove(&keys))
                .unwrap_or(0)
                .into(),
            ZSetOps::ZRange(zset_key, start, stop) => read_zsets!(state, &zset_key)
                .map(|zset| {
                    let (start, stop) = deal_with_negative_indices(zset.card(), (start, stop));
                    println!("{}, {}", start, stop);
                    zset.range((start, stop))
                        .into_iter()
                        .map(|item| item.member)
                        .collect()
                })
                .unwrap_or_else(Vec::new)
                .into(),
            ZSetOps::ZCard(zset_key) => read_zsets!(state, &zset_key)
                .map(|zset| zset.card())
                .unwrap_or(0)
                .into(),
            ZSetOps::ZScore(zset_key, member_key) => read_zsets!(state, &zset_key)
                .and_then(|zset| zset.score(member_key))
                .map(ReturnValue::IntRes)
                .unwrap_or(ReturnValue::Nil)
                .into(),
            ZSetOps::ZPopMax(zset_key, count) => write_zsets!(state, &zset_key)
                .map(|zset| {
                    zset.pop_max(Some(count))
                        .into_iter()
                        .fold(Vec::new(), |mut acc, zset_mem| {
                            acc.push(ReturnValue::IntRes(zset_mem.score));
                            acc.push(ReturnValue::StringRes(zset_mem.member.into_bytes()));
                            acc
                        })
                })
                .map(ReturnValue::Array)
                .unwrap_or_else(|| ReturnValue::Array(vec![]))
                .into(),
            ZSetOps::ZPopMin(zset_key, count) => write_zsets!(state, &zset_key)
                .map(|zset| {
                    zset.pop_min(Some(count))
                        .into_iter()
                        .fold(Vec::new(), |mut acc, zset_mem| {
                            acc.push(ReturnValue::IntRes(zset_mem.score));
                            acc.push(ReturnValue::StringRes(zset_mem.member.into_bytes()));
                            acc
                        })
                })
                .map(ReturnValue::Array)
                .unwrap_or_else(|| ReturnValue::Array(vec![]))
                .into(),
            ZSetOps::ZRank(zset_key, mem_key) => read_zsets!(state, &zset_key)
                .and_then(|zset| zset.rank(mem_key))
                .map(ReturnValue::IntRes)
                .unwrap_or(ReturnValue::Nil)
                .into(),
        }
    }
}
