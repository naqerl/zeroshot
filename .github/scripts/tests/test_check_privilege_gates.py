"""Focused tests for the privileged-test gate checker."""

from __future__ import annotations

import importlib.util
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
CHECKER = ROOT / ".github" / "scripts" / "check_privilege_gates.py"


def load_checker():
    spec = importlib.util.spec_from_file_location("check_privilege_gates", CHECKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checker = load_checker()

GOOD_LANE = "run: sudo -n env ZEROSHOT_REQUIRE_USER_NAMESPACES=1 test-bin root_ --test-threads=1\n"
BROKEN_LANE = "run: sudo -n env ZEROSHOT_REQUIRE_USER_NAMESPACES=1 test-bin --test-threads=1\n"


class CheckerTree:
    """A temporary repository tree the checker can walk."""

    def __init__(self, source, workflows=None):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        source_root = self.root / "zeroshot" / "src"
        source_root.mkdir(parents=True)
        (source_root / "sample.rs").write_text(source, encoding="utf-8")
        workflow_root = self.root / ".github" / "workflows"
        workflow_root.mkdir(parents=True)
        workflows = workflows or {}
        paths = []
        for name in ("ci.yml", "coverage.yml"):
            path = workflow_root / name
            path.write_text(workflows.get(name, GOOD_LANE), encoding="utf-8")
            paths.append(path)
        self.source_root = source_root
        self.workflows = tuple(paths)

    def run(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.multiple(
                checker,
                ROOT=self.root,
                SOURCE_ROOT=self.source_root,
                WORKFLOWS=self.workflows,
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            status = checker.main()
        return status, stdout.getvalue(), stderr.getvalue()

    def gated(self, source):
        path = self.root / "zeroshot" / "src" / "gates.rs"
        path.write_text(source, encoding="utf-8")
        return checker.gated_tests(path)


HARD_GATE = """\
#[test]
fn {}() {{
    if unsafe {{ libc::geteuid() }} != 0 {{
        eprintln!("skip");
        return;
    }}
    assert!(true);
}}
"""


class PrivilegeGateCheckerTests(unittest.TestCase):
    def tree(self, source, workflows=None):
        tree = CheckerTree(source, workflows)
        self.addCleanup(tree.temporary.cleanup)
        return tree

    def test_flags_a_misnamed_hard_gate(self):
        tree = self.tree(HARD_GATE.format("ordinary_name"))
        status, stdout, _ = tree.run()
        self.assertEqual(status, 1)
        self.assertIn("ordinary_name: root-gated test must be named root_*", stdout)

    def test_accepts_a_correctly_named_hard_gate(self):
        tree = self.tree(HARD_GATE.format("root_named_gate"))
        status, stdout, _ = tree.run()
        self.assertEqual(status, 0)
        self.assertIn("checked 1 privileged tests, 0 violations", stdout)

    def test_partial_root_only_blocks_are_accepted_in_both_directions(self):
        cases = {
            "root-only work": """\
#[test]
fn partial_root_block() {
    if unsafe { libc::geteuid() } == 0 {
        assert!(true);
    }
    assert!(true);
}
""",
            "non-root work": """\
#[test]
fn partial_non_root_block() {
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("not privileged");
    }
    assert!(true);
}
""",
            "nested early return": """\
#[test]
fn partial_nested_return() {
    if unsafe { libc::geteuid() } != 0 {
        if cfg!(target_os = "linux") {
            return;
        }
    }
    assert!(true);
}
""",
        }
        for name, source in cases.items():
            with self.subTest(name):
                self.assertEqual(self.tree(source).gated(source), [])

    def test_commented_out_gate_is_not_a_gate(self):
        source = """\
#[test]
fn commented_gate() {
    // if unsafe { libc::geteuid() } != 0 { return; }
    /* if unsafe { libc::geteuid() } != 0 { return; } */
    assert!(true);
}
"""
        self.assertEqual(self.tree(source).gated(source), [])

    def test_gate_inside_a_string_literal_is_not_a_gate(self):
        source = """\
#[test]
fn string_gate() {
    let gate = "if unsafe { libc::geteuid() } != 0 { return; }";
    let raw = r#"if unsafe { libc::geteuid() } != 0 { return; }"#;
    assert!(!gate.is_empty() && !raw.is_empty());
}
"""
        self.assertEqual(self.tree(source).gated(source), [])

    def test_a_lane_without_the_root_filter_is_rejected(self):
        tree = self.tree(
            HARD_GATE.format("root_named_gate"),
            {"coverage.yml": BROKEN_LANE},
        )
        status, _, stderr = tree.run()
        self.assertEqual(status, 3)
        self.assertIn("coverage.yml no longer runs the lib test binary", stderr)

    def test_each_lane_without_the_root_filter_is_rejected(self):
        for name in ("ci.yml", "coverage.yml"):
            with self.subTest(name):
                tree = self.tree(
                    HARD_GATE.format("root_named_gate"),
                    {name: BROKEN_LANE},
                )
                status, _, stderr = tree.run()
                self.assertEqual(status, 3)
                self.assertIn(f"{name} no longer runs the lib test binary", stderr)

    def test_no_gates_fails_instead_of_passing_silently(self):
        source = """\
#[test]
fn ordinary_test() {
    assert!(true);
}
"""
        status, _, stderr = self.tree(source).run()
        self.assertEqual(status, 2)
        self.assertIn("found no root-gated tests", stderr)


if __name__ == "__main__":
    unittest.main()
