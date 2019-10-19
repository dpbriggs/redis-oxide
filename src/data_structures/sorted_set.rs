use crate::types::{Count, Index, Key, Score};
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct SortedSetMember {
    pub score: Score,
    pub member: String,
}

impl PartialOrd<SortedSetMember> for SortedSetMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let score_cmp = self.score.cmp(&other.score);
        if let Ordering::Equal = score_cmp {
            return Some(self.member.cmp(&other.member));
        }
        Some(score_cmp)
    }
}

impl Ord for SortedSetMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

// TODO: Look into using RangeBounds properly
// impl RangeBounds<Score> for SortedSetMember {
//     fn start_bound(&self) -> Bound<&Score> {
//         Included(&self.score)
//     }

//     fn end_bound(&self) -> Bound<&Score> {
//         Included(&self.score)
//     }
//     fn contains<U>(&self, item: &U) -> bool
//     where
//         U: PartialOrd<Score> + ?Sized,
//     {
//         if let Ordering::Equal = item.partial_cmp(&self.score).unwrap() {
//             true
//         } else {
//             false
//         }
//     }
// }

// impl RangeBounds<SortedSetMember> for Range<Score> {
//     fn start_bound(&self) -> Bound<&SortedSetMember> {
//         let f = SortedSetMember::new(&b"".to_vec(), self.start);
//         Included(f)
//     }
//     fn end_bound(&self) -> Bound<&SortedSetMember> {
//         let f = SortedSetMember::new(&b"".to_vec(), self.end);
//         Included(&f)
//     }

// fn contains<U>(&self, item: &U) -> bool
// where
//     U: PartialOrd<Score> + ?Sized {
//     if let Ordering::Equal = item.partial_cmp(&self.score).unwrap() {
//         true
//     } else {
//         false
//     }
// }
// }

impl SortedSetMember {
    fn new(key: &[u8], score: Score) -> Self {
        SortedSetMember {
            score,
            member: String::from_utf8_lossy(key).to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SortedSet {
    members_hash: HashMap<Key, Score>,
    scores: BTreeSet<SortedSetMember>,
}

#[allow(unused)]
impl SortedSet {
    /// Create a new SortedSet
    pub fn new() -> Self {
        SortedSet::default()
    }

    /// Add the following keys and scores to the sorted set
    pub fn add(&mut self, key_scores: Vec<(Score, Key)>) -> Count {
        key_scores
            .into_iter()
            .map(|(score, key)| match self.members_hash.entry(key) {
                Entry::Vacant(ent) => {
                    self.scores.insert(SortedSetMember::new(ent.key(), score));
                    ent.insert(score);
                    1
                }
                Entry::Occupied(_) => 0,
            })
            .sum()
    }

    /// Remove the following keys from the sorted set
    pub fn remove(&mut self, keys: &[Key]) -> Count {
        keys.iter()
            .map(|key| match self.members_hash.remove(key) {
                None => 0,
                Some(score) => {
                    let tmp = SortedSetMember::new(key, score);
                    self.scores.remove(&tmp);
                    1
                }
            })
            .sum()
    }

    /// Returns the number of members stored in the set.
    pub fn card(&self) -> Count {
        self.members_hash.len() as Count
    }

    /// Return the score of the member in the sorted set
    pub fn score(&self, key: Key) -> Option<Score> {
        self.members_hash.get(&key).cloned()
    }

    /// Get all members between (lower, upper) scores
    pub fn range(&self, range: (Score, Score)) -> Vec<SortedSetMember> {
        // TODO: Use a more efficient method. I should use a skiplist or an AVL tree.
        // Another option is to retackle the rangebounds stuff, but the semantics are different.
        // I want to be able to compare by score AND member when inserting/removing,
        // but only by score in this case. Need to figure out how to encode that.
        self.scores
            .iter()
            .filter(|mem| range.0 <= mem.score && mem.score <= range.1)
            .cloned()
            .collect()
    }

    /// Remove count (default: 1) maximum members from the sorted set
    pub fn pop_max(&mut self, count: Option<Count>) -> Vec<SortedSetMember> {
        let count = count.unwrap_or(1) as usize;
        let ret: Vec<SortedSetMember> = self.scores.iter().rev().take(count).cloned().collect();
        let keys_to_remove: Vec<Key> = ret.iter().map(|s| s.member.as_bytes().to_vec()).collect();
        self.remove(&keys_to_remove);
        ret
    }

    /// Remove count (default: 1) minimum members from the sorted set
    pub fn pop_min(&mut self, count: Option<Count>) -> Vec<SortedSetMember> {
        let count = count.unwrap_or(1) as usize;
        let ret: Vec<SortedSetMember> = self.scores.iter().take(count).cloned().collect();
        let keys_to_remove: Vec<Key> = ret.iter().map(|s| s.member.as_bytes().to_vec()).collect();
        self.remove(&keys_to_remove);
        ret
    }

    /// Get the maximum score in the sorted set
    pub fn max_score(&self) -> Option<Score> {
        self.scores.iter().rev().next().cloned().map(|m| m.score)
    }

    /// Get the rank of a given key in the sorted set
    pub fn rank(&self, key: Key) -> Option<Index> {
        self.scores
            .iter()
            .position(|s| s.member.as_bytes() == &*key)
            .map(|pos| pos as Index)
    }
}

#[cfg(test)]
mod test_sorted_sets_ds {
    use crate::data_structures::sorted_set::{SortedSet, SortedSetMember};
    use crate::types::{Key, Score};

    fn get_multiple_entries() -> Vec<(Score, Key)> {
        vec![
            (1, b"hi_0".to_vec()),
            (3, b"hi_1".to_vec()),
            (5, b"hi_2".to_vec()),
        ]
    }

    fn get_multiple_sorted_set_entries() -> Vec<SortedSetMember> {
        get_multiple_entries()
            .into_iter()
            .map(|(score, key)| SortedSetMember::new(&key, score))
            .collect()
    }

    #[test]
    fn test_add() {
        let mut ss = SortedSet::new();
        assert_eq!(1, ss.add(vec![(2, b"hi".to_vec())]));
        assert_eq!(
            get_multiple_entries().len() as i64,
            ss.add(get_multiple_entries())
        );
        assert_eq!(0, ss.add(get_multiple_entries()));
    }

    #[test]
    fn test_range() {
        let mut ss = SortedSet::new();

        ss.add(vec![
            (1, b"hi_0".to_vec()),
            (3, b"hi_1".to_vec()),
            (5, b"hi_2".to_vec()),
        ]);
        assert_eq!(
            ss.range((1, 5)),
            vec![
                SortedSetMember::new(&b"hi_0".to_vec(), 1),
                SortedSetMember::new(&b"hi_1".to_vec(), 3),
                SortedSetMember::new(&b"hi_2".to_vec(), 5),
            ]
        );
        assert_eq!(
            ss.range((2, 4)),
            vec![SortedSetMember::new(&b"hi_1".to_vec(), 3),]
        );
        let empty_vec: Vec<SortedSetMember> = Vec::new();
        assert_eq!(ss.range((20, 40)), empty_vec);
    }

    #[test]
    fn test_remove() {
        let mut ss = SortedSet::new();
        let all_keys: Vec<Key> = get_multiple_entries()
            .into_iter()
            .map(|(_, key)| key)
            .collect();
        assert_eq!(0, ss.remove(&all_keys.clone()));
        ss.add(get_multiple_entries());
        assert_eq!(1, ss.remove(&[all_keys[1].clone()]));
        assert_eq!(2, ss.card());
        assert_eq!(2, ss.remove(&all_keys));
        assert_eq!(0, ss.card());
    }

    #[test]
    fn test_pop_max() {
        let mut ss = SortedSet::new();
        assert_eq!(ss.pop_max(None), Vec::new());
        assert_eq!(ss.pop_max(Some(10)), Vec::new());
        ss.add(get_multiple_entries());
        let entries = get_multiple_sorted_set_entries();
        assert_eq!(ss.pop_max(None), vec![entries.last().unwrap().clone()]);
        let first_two: Vec<SortedSetMember> = entries.iter().rev().skip(1).cloned().collect();
        assert_eq!(ss.pop_max(Some(2)), first_two);
        assert_eq!(ss.pop_max(Some(2)), Vec::new());
    }
    #[test]
    fn test_pop_min() {
        let mut ss = SortedSet::new();
        assert_eq!(ss.pop_min(None), Vec::new());
        assert_eq!(ss.pop_min(Some(10)), Vec::new());
        ss.add(get_multiple_entries());
        let entries = get_multiple_sorted_set_entries();
        assert_eq!(ss.pop_min(None), vec![entries.first().unwrap().clone()]);
        let last_two: Vec<SortedSetMember> = entries.iter().skip(1).cloned().collect();
        assert_eq!(ss.pop_min(Some(2)), last_two);
        assert_eq!(ss.pop_min(Some(2)), Vec::new());
    }
}
