//! Client read masks.
//!
//! The gateway holds checkpoints under the canonical mask. A client mask is
//! applied to that canonical checkpoint, so parent paths (`transactions`,
//! `*`) mean "everything the gateway has below this path". Explicit paths to
//! fields outside the canonical set are rejected rather than silently
//! returned empty.

use std::sync::Arc;

use sui_rpc::field::{FieldMask, FieldMaskTree, FieldMaskUtil};
use sui_rpc::merge::Merge;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;

/// A validated client mask.
#[derive(Clone, Debug)]
pub enum Mask {
    /// Covers every canonical path: send the canonical checkpoint as-is.
    Full,
    /// Anything narrower. `key` is the normalized path list.
    Partial {
        key: Arc<str>,
        tree: Arc<FieldMaskTree>,
    },
}

/// Cache key for rendered output.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MaskKey {
    Full,
    Partial(Arc<str>),
}

impl Mask {
    /// Parse a comma-separated `read_mask`; absent or empty → full.
    pub fn parse(param: Option<&str>) -> Result<Self, String> {
        let paths: Vec<&str> = param
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .collect();
        if paths.is_empty() || paths.contains(&"*") {
            return Ok(Self::Full);
        }
        let mask = FieldMask::from_paths(&paths);
        mask.validate::<Checkpoint>()
            .map_err(|p| format!("invalid read_mask path {p:?}"))?;
        for path in &paths {
            if let Some(excluded) = common::EXCLUDED_PATHS.iter().find(|ex| covers(ex, path)) {
                return Err(format!(
                    "read_mask path {path:?} is not served (excluded: {excluded})"
                ));
            }
        }
        if common::CANONICAL_READ_MASK
            .iter()
            .all(|c| paths.iter().any(|p| covers(p, c)))
        {
            return Ok(Self::Full);
        }
        let tree = FieldMaskTree::from_field_mask(&mask);
        let key = tree.to_field_mask().display().to_string();
        Ok(Self::Partial {
            key: key.into(),
            tree: Arc::new(tree),
        })
    }

    pub fn key(&self) -> MaskKey {
        match self {
            Self::Full => MaskKey::Full,
            Self::Partial { key, .. } => MaskKey::Partial(key.clone()),
        }
    }

    /// Apply to a canonical checkpoint.
    pub fn apply(&self, checkpoint: &Checkpoint) -> Checkpoint {
        match self {
            Self::Full => checkpoint.clone(),
            Self::Partial { tree, .. } => Checkpoint::merge_from(checkpoint, tree),
        }
    }
}

/// Whether mask path `parent` includes field path `path`.
fn covers(parent: &str, path: &str) -> bool {
    path == parent
        || path
            .strip_prefix(parent)
            .is_some_and(|rest| rest.starts_with('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_masks() {
        assert!(matches!(Mask::parse(None), Ok(Mask::Full)));
        assert!(matches!(Mask::parse(Some("")), Ok(Mask::Full)));
        assert!(matches!(Mask::parse(Some("*")), Ok(Mask::Full)));
        assert!(matches!(
            Mask::parse(Some(
                "sequence_number,digest,summary,signature,contents,transactions,objects"
            )),
            Ok(Mask::Full)
        ));
        let canonical = common::CANONICAL_READ_MASK.join(",");
        assert!(matches!(Mask::parse(Some(&canonical)), Ok(Mask::Full)));
    }

    #[test]
    fn partial_masks_normalize() {
        let a = Mask::parse(Some("digest,sequence_number")).unwrap();
        let b = Mask::parse(Some(" sequence_number , digest ")).unwrap();
        assert_eq!(a.key(), b.key());
        assert!(matches!(a, Mask::Partial { .. }));
        // Redundant child paths collapse into the parent.
        let c = Mask::parse(Some("summary,summary.epoch")).unwrap();
        assert_eq!(c.key(), MaskKey::Partial("summary".into()));
    }

    #[test]
    fn rejects_invalid_and_excluded() {
        assert!(Mask::parse(Some("nope")).is_err());
        assert!(Mask::parse(Some("digest.x")).is_err());
        assert!(Mask::parse(Some("transactions.events.events.json")).is_err());
        assert!(Mask::parse(Some("objects.objects.display")).is_err());
        assert!(Mask::parse(Some("objects.objects.json")).is_err());
        // Parents of excluded paths are fine: they mean "what is served".
        assert!(Mask::parse(Some("transactions.events")).is_ok());
        assert!(Mask::parse(Some("objects")).is_ok());
    }

    #[test]
    fn apply_partial() {
        let mut cp = Checkpoint::default();
        cp.sequence_number = Some(7);
        cp.digest = Some("d".into());
        cp.signature = Some(Default::default());
        let m = Mask::parse(Some("sequence_number")).unwrap();
        let out = m.apply(&cp);
        assert_eq!(out.sequence_number, Some(7));
        assert_eq!(out.digest, None);
        assert_eq!(out.signature, None);
        assert_eq!(Mask::Full.apply(&cp), cp);
    }
}
