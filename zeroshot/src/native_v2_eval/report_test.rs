use super::*;

#[test]
fn markdown_table_cells_escape_backslashes_pipes_and_controls() {
    assert_eq!(escape(r"C:\|value"), r"C:\\\|value");
    assert_eq!(escape("tab\tline\nbreak\r"), "tab line break ");
    let normalized = escape("safe\u{202e}text\u{2066}tail");
    assert!(!normalized.contains('\u{202e}'));
    assert!(!normalized.contains('\u{2066}'));
    assert_eq!(normalized, "safe text tail");
    assert_eq!(judge_label(Some("graph|local")), "graph\\|local");
}
