//! Pop-out windows as the shell tracks them: a flow's notebook, data preview, logs or AI assistant
//! in a window of its own. The shell knows no kinds: the renderer names one (a plain identifier)
//! and every window is labelled `popout-<kind>-<seq>`. Tauri labels are immutable and a Save As
//! moves a flow to a new id, so the label is the key and this registry carries the `(kind, flow
//! id)` a window hosts right now.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

pub const LABEL_PREFIX: &str = "popout-";

const MAX_KIND_LEN: usize = 32;

pub fn is_popout_label(label: &str) -> bool {
    label.starts_with(LABEL_PREFIX)
}

/// A kind is an identifier (`[a-z][a-z0-9_]*`, at most 32 chars): a label never has to be parsed,
/// and the capability glob `popout-*` is the only thing that reads one.
pub fn validate_kind(kind: &str) -> Result<(), String> {
    let mut chars = kind.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    let rest_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if first_ok && rest_ok && kind.len() <= MAX_KIND_LEN {
        Ok(())
    } else {
        Err(format!("'{kind}' is not a pop-out kind"))
    }
}

/// What a pop-out window hosts; serialises as the renderer's `{ kind, flowId }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PopoutRef {
    pub kind: String,
    pub flow_id: i64,
}

/// Asking for the window of `(kind, flow_id)`: the label already hosting it, or a fresh one.
#[derive(Debug, PartialEq, Eq)]
pub enum Registration {
    Existing(String),
    New(String),
}

#[derive(Debug, Default)]
pub struct PopoutRegistry {
    seq: u64,
    windows: HashMap<String, PopoutRef>,
    /// Labels registered whose window is not built yet, so an open can tell a window still being
    /// built (share it) from one already destroyed whose close has not been reported (replace it).
    building: HashSet<String>,
}

impl PopoutRegistry {
    /// The label hosting `(kind, flow_id)`, else a new label allocated for it. Lookup and insert
    /// happen under the caller's one lock, so two concurrent opens of a pair share one window.
    pub fn find_or_register(&mut self, kind: &str, flow_id: i64) -> Registration {
        if let Some(label) = self.find(kind, flow_id) {
            return Registration::Existing(label);
        }
        self.seq += 1;
        let label = format!("{LABEL_PREFIX}{kind}-{}", self.seq);
        let popout = PopoutRef {
            kind: kind.to_string(),
            flow_id,
        };
        self.windows.insert(label.clone(), popout);
        self.building.insert(label.clone());
        Registration::New(label)
    }

    /// The window of a label from `find_or_register` exists now.
    pub fn mark_built(&mut self, label: &str) {
        self.building.remove(label);
    }

    pub fn is_building(&self, label: &str) -> bool {
        self.building.contains(label)
    }

    pub fn get(&self, label: &str) -> Option<&PopoutRef> {
        self.windows.get(label)
    }

    /// The label of the window hosting `(kind, flow_id)`, if one does.
    pub fn find(&self, kind: &str, flow_id: i64) -> Option<String> {
        self.windows
            .iter()
            .find(|(_, popout)| popout.kind == kind && popout.flow_id == flow_id)
            .map(|(label, _)| label.clone())
    }

    /// The window's flow moved to another id (a Save As). Refused for an unknown label and when
    /// another window of that kind already hosts `to`; returns what the window hosted before.
    pub fn rekey(&mut self, label: &str, to: i64) -> Result<PopoutRef, String> {
        let before = self
            .get(label)
            .cloned()
            .ok_or_else(|| format!("window '{label}' is not a pop-out window"))?;
        if self
            .find(&before.kind, to)
            .is_some_and(|other| other != label)
        {
            return Err(format!("flow {to} already has a {} window", before.kind));
        }
        if let Some(popout) = self.windows.get_mut(label) {
            popout.flow_id = to;
        }
        Ok(before)
    }

    pub fn remove(&mut self, label: &str) -> Option<PopoutRef> {
        self.building.remove(label);
        self.windows.remove(label)
    }

    /// Every registered window with its label, by kind then flow id.
    pub fn list(&self) -> Vec<(String, PopoutRef)> {
        let mut windows: Vec<(String, PopoutRef)> = self
            .windows
            .iter()
            .map(|(label, popout)| (label.clone(), popout.clone()))
            .collect();
        windows.sort_by(|(_, a), (_, b)| a.kind.cmp(&b.kind).then(a.flow_id.cmp(&b.flow_id)));
        windows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn popout(kind: &str, flow_id: i64) -> PopoutRef {
        PopoutRef {
            kind: kind.to_string(),
            flow_id,
        }
    }

    #[test]
    fn kinds_are_plain_identifiers() {
        for kind in ["notebook", "ai", "table_2"] {
            assert!(validate_kind(kind).is_ok(), "{kind}");
        }
        let long = "a".repeat(MAX_KIND_LEN + 1);
        for kind in [
            "",
            "Notebook",
            "note-book",
            "../x",
            "2table",
            "a b",
            long.as_str(),
        ] {
            assert!(validate_kind(kind).is_err(), "{kind:?}");
        }
    }

    #[test]
    fn labels_carry_the_prefix() {
        assert!(is_popout_label("popout-notebook-1"));
        assert!(!is_popout_label("main"));
        assert!(!is_popout_label("notebook-4"));
    }

    #[test]
    fn a_ref_serialises_as_the_renderer_reads_it() {
        assert_eq!(
            serde_json::to_value(popout("notebook", 4)).unwrap(),
            serde_json::json!({ "kind": "notebook", "flowId": 4 })
        );
    }

    #[test]
    fn one_window_per_pair_until_it_is_gone() {
        let mut registry = PopoutRegistry::default();
        let first = "popout-notebook-1".to_string();
        assert_eq!(
            registry.find_or_register("notebook", 4),
            Registration::New(first.clone())
        );
        assert_eq!(
            registry.find_or_register("notebook", 4),
            Registration::Existing(first.clone())
        );
        assert_eq!(
            registry.find_or_register("logs", 4),
            Registration::New("popout-logs-2".to_string())
        );
        assert_eq!(registry.find("notebook", 4), Some(first.clone()));

        assert_eq!(registry.remove(&first), Some(popout("notebook", 4)));
        assert_eq!(registry.find("notebook", 4), None);
        assert_eq!(registry.remove(&first), None);
        assert_eq!(
            registry.find_or_register("notebook", 4),
            Registration::New("popout-notebook-3".to_string())
        );
    }

    #[test]
    fn a_label_is_building_until_marked_built_and_again_when_reused() {
        let mut registry = PopoutRegistry::default();
        let Registration::New(label) = registry.find_or_register("table", 4) else {
            panic!("fresh registry");
        };
        assert!(registry.is_building(&label));
        registry.mark_built(&label);
        assert!(!registry.is_building(&label));
        assert!(!registry.is_building("popout-table-99"));

        registry.remove(&label);
        let Registration::New(again) = registry.find_or_register("table", 4) else {
            panic!("removed");
        };
        assert_ne!(again, label);
        assert!(registry.is_building(&again));
    }

    #[test]
    fn rekey_moves_the_window_to_the_new_flow() {
        let mut registry = PopoutRegistry::default();
        let Registration::New(label) = registry.find_or_register("logs", 4) else {
            panic!("fresh registry");
        };
        assert_eq!(registry.rekey(&label, 9), Ok(popout("logs", 4)));
        assert_eq!(registry.find("logs", 4), None);
        assert_eq!(registry.find("logs", 9), Some(label.clone()));
        assert_eq!(registry.get(&label), Some(&popout("logs", 9)));
        assert_eq!(registry.rekey(&label, 9), Ok(popout("logs", 9)));
        assert!(registry.rekey("popout-logs-99", 1).is_err());
    }

    #[test]
    fn rekey_refuses_a_flow_another_window_of_that_kind_hosts() {
        let mut registry = PopoutRegistry::default();
        let Registration::New(label) = registry.find_or_register("logs", 4) else {
            panic!("fresh registry");
        };
        registry.find_or_register("logs", 9);
        registry.find_or_register("notebook", 9);
        let refused = registry.rekey(&label, 9).unwrap_err();
        assert!(refused.contains("already has a logs window"), "{refused}");
        assert_eq!(registry.get(&label), Some(&popout("logs", 4)));
        assert_eq!(registry.find("logs", 9), Some("popout-logs-2".to_string()));
    }

    #[test]
    fn lists_by_kind_then_flow() {
        let mut registry = PopoutRegistry::default();
        registry.find_or_register("notebook", 7);
        registry.find_or_register("ai", 3);
        registry.find_or_register("notebook", 2);
        let listed: Vec<(String, i64)> = registry
            .list()
            .into_iter()
            .map(|(_, p)| (p.kind, p.flow_id))
            .collect();
        assert_eq!(
            listed,
            vec![
                ("ai".into(), 3),
                ("notebook".into(), 2),
                ("notebook".into(), 7)
            ]
        );
    }
}
