//! Pop-out windows as the shell tracks them: a flow's notebook, data preview, logs or AI assistant
//! in a window of its own. The shell knows no kinds: the renderer names one (a plain identifier)
//! and every window is labelled `popout-<kind>-<seq>`. Tauri labels are immutable and a Save As
//! moves a flow to a new id, so the label is the key and this registry carries the `(kind, flow
//! id)` a window hosts right now.

use std::collections::HashMap;

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

#[derive(Debug, Default)]
pub struct PopoutRegistry {
    seq: u64,
    windows: HashMap<String, PopoutRef>,
}

impl PopoutRegistry {
    /// Allocate the label of a new window hosting `(kind, flow_id)`.
    pub fn register(&mut self, kind: &str, flow_id: i64) -> String {
        self.seq += 1;
        let label = format!("{LABEL_PREFIX}{kind}-{}", self.seq);
        let popout = PopoutRef {
            kind: kind.to_string(),
            flow_id,
        };
        self.windows.insert(label.clone(), popout);
        label
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

    /// The window's flow moved to another id (a Save As); false when the label is unknown.
    pub fn rekey(&mut self, label: &str, to: i64) -> bool {
        match self.windows.get_mut(label) {
            Some(popout) => {
                popout.flow_id = to;
                true
            }
            None => false,
        }
    }

    pub fn remove(&mut self, label: &str) -> Option<PopoutRef> {
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

    #[test]
    fn kinds_are_plain_identifiers() {
        for kind in ["notebook", "ai", "table_2"] {
            assert!(validate_kind(kind).is_ok(), "{kind}");
        }
        let long = "a".repeat(MAX_KIND_LEN + 1);
        for kind in ["", "Notebook", "note-book", "../x", "2table", "a b", long.as_str()] {
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
        let popout = PopoutRef {
            kind: "notebook".into(),
            flow_id: 4,
        };
        assert_eq!(
            serde_json::to_value(popout).unwrap(),
            serde_json::json!({ "kind": "notebook", "flowId": 4 })
        );
    }

    #[test]
    fn registers_distinct_labels_and_finds_the_live_one() {
        let mut registry = PopoutRegistry::default();
        let first = registry.register("notebook", 4);
        assert_eq!(first, "popout-notebook-1");
        assert_eq!(registry.find("notebook", 4), Some(first.clone()));
        assert_eq!(registry.find("logs", 4), None);

        let removed = registry.remove(&first);
        assert_eq!(removed.map(|p| p.flow_id), Some(4));
        assert_eq!(registry.find("notebook", 4), None);
        assert_eq!(registry.remove(&first), None);

        let second = registry.register("notebook", 4);
        assert_eq!(second, "popout-notebook-2");
        assert_eq!(registry.find("notebook", 4), Some(second));
    }

    #[test]
    fn rekey_moves_the_window_to_the_new_flow() {
        let mut registry = PopoutRegistry::default();
        let label = registry.register("logs", 4);
        assert!(registry.rekey(&label, 9));
        assert_eq!(registry.find("logs", 4), None);
        assert_eq!(registry.find("logs", 9), Some(label.clone()));
        assert_eq!(registry.get(&label).map(|p| p.flow_id), Some(9));
        assert!(!registry.rekey("popout-logs-99", 1));
    }

    #[test]
    fn lists_by_kind_then_flow() {
        let mut registry = PopoutRegistry::default();
        registry.register("notebook", 7);
        registry.register("ai", 3);
        registry.register("notebook", 2);
        let listed: Vec<(String, i64)> = registry
            .list()
            .into_iter()
            .map(|(_, p)| (p.kind, p.flow_id))
            .collect();
        assert_eq!(
            listed,
            vec![("ai".into(), 3), ("notebook".into(), 2), ("notebook".into(), 7)]
        );
    }
}
