use super::*;

#[test]
fn markdown_table_cells_escape_backslashes_pipes_and_controls() {
    assert_eq!(escape(r"C:\|value"), r"C:\\\|value");
    assert_eq!(escape("tab\tline\nbreak\r"), "tab line break ");
    let normalized = escape("safe\u{202e}text\u{2066}tail");
    assert!(!normalized.contains('\u{202e}'));
    assert!(!normalized.contains('\u{2066}'));
    assert_eq!(normalized, "safe text tail");
}

#[test]
fn markdown_code_spans_survive_backticks_pipes_and_backslashes() {
    assert_eq!(
        judge_label(Some("model`with`ticks"), Some("fixture")),
        "``model`with`ticks`` (fixture)"
    );
    assert_eq!(code_span("gateway|model"), "`gateway\\|model`");
    assert_eq!(code_span("`edge"), "`` `edge ``");
    assert_eq!(
        judge_label(Some(r"C:\|model"), Some("graph")),
        r"`C:\\\|model` (graph)"
    );
    assert_eq!(model_suffix(Some(r"C:\|model")), r" (`C:\\\|model`)");
}
