use crate::ops::RVec;
use crate::types::{Count, Index, Key, ReturnValue, Score, StateRef};
use crate::{make_reader, make_writer, op_variants};

op_variants! {
    ZSetOps,
    ZAdd(Key, RVec<(Score, Key)>),
    ZRem(Key, RVec<Key>),
    ZRange(Key, Score, Score),
    ZCard(Key),
    ZScore(Key, Key),
    ZPopMax(Key, Count),
    ZPopMin(Key, Count),
    ZRank(Key, Key)
}

make_reader!(zsets, read_zsets);
make_writer!(zsets, write_zsets);

fn deal_with_negative_indices(coll_size: Count, bounds: (Index, Index)) -> (Index, Index) {
    let (start, end) = bounds;
    let start = if start < 0 { start + coll_size } else { start };
    let end = if end < 0 { end + coll_size } else { end };
    (start, end)
}

pub async fn zset_interact(zset_op: ZSetOps, state: StateRef) -> ReturnValue {
    match zset_op {
        ZSetOps::ZAdd(zset_key, member_scores) => {
            let mut zset = state.zsets.entry(zset_key).or_default();
            let num_added = zset.add(member_scores);
            ReturnValue::IntRes(num_added)
        }
        ZSetOps::ZRem(zset_key, keys) => write_zsets!(state, &zset_key)
            .map(|mut zset| zset.remove(&keys))
            .unwrap_or(0)
            .into(),
        ZSetOps::ZRange(zset_key, start, stop) => read_zsets!(state, &zset_key)
            .map(|zset| {
                let (start, stop) = deal_with_negative_indices(zset.card(), (start, stop));
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
            .unwrap_or(ReturnValue::Nil),
        ZSetOps::ZPopMax(zset_key, count) => write_zsets!(state, &zset_key)
            .map(|mut zset| {
                zset.pop_max(count)
                    .into_iter()
                    .fold(Vec::new(), |mut acc, zset_mem| {
                        acc.push(ReturnValue::IntRes(zset_mem.score));
                        acc.push(ReturnValue::StringRes(zset_mem.member.into()));
                        acc
                    })
            })
            .map(ReturnValue::Array)
            .unwrap_or_else(|| ReturnValue::Array(vec![])),
        ZSetOps::ZPopMin(zset_key, count) => write_zsets!(state, &zset_key)
            .map(|mut zset| {
                zset.pop_min(count)
                    .into_iter()
                    .fold(Vec::new(), |mut acc, zset_mem| {
                        acc.push(ReturnValue::IntRes(zset_mem.score));
                        acc.push(ReturnValue::StringRes(zset_mem.member.into()));
                        acc
                    })
            })
            .map(ReturnValue::Array)
            .unwrap_or_else(|| ReturnValue::Array(vec![])),
        ZSetOps::ZRank(zset_key, mem_key) => read_zsets!(state, &zset_key)
            .and_then(|zset| zset.rank(mem_key))
            .map(ReturnValue::IntRes)
            .unwrap_or(ReturnValue::Nil),
    }
}
