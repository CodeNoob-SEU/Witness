"""Unit tests for the unified-diff parser.

The parser is the one place in the patch path that does real text handling, so
it is tested against hand-written diffs rather than only through a live run.
Line numbering is the part that silently goes wrong, so it is asserted
explicitly on both sides of every hunk.
"""

from __future__ import annotations

from react_agent.patch import MAX_PATCH_FILES, parse_unified_diff


def test_line_numbers_advance_independently_on_each_side() -> None:
    diff = (
        "diff --git a/app.py b/app.py\n"
        "index 1111111..2222222 100644\n"
        "--- a/app.py\n"
        "+++ b/app.py\n"
        "@@ -10,4 +10,5 @@ def handler():\n"
        " before\n"
        "-old\n"
        "+new one\n"
        "+new two\n"
        " after\n"
    )

    files, truncated = parse_unified_diff(diff)

    assert not truncated
    assert len(files) == 1
    patch = files[0]
    assert patch.path == "app.py"
    assert patch.change == "modified"
    assert (patch.additions, patch.deletions) == (2, 1)

    kinds = [(line.kind, line.old_line, line.new_line) for line in patch.hunks[0].lines]
    assert kinds == [
        ("context", 10, 10),
        ("removed", 11, None),
        ("added", None, 11),
        ("added", None, 12),
        # The context line after the edit sits at 12 on the old side and 13 on
        # the new side: a parser that shares one counter gets this wrong.
        ("context", 12, 13),
    ]


def test_a_hunk_header_without_counts_defaults_to_one_line() -> None:
    diff = (
        "diff --git a/one.txt b/one.txt\n"
        "--- a/one.txt\n"
        "+++ b/one.txt\n"
        "@@ -3 +3 @@\n"
        "-was\n"
        "+is\n"
    )

    files, _ = parse_unified_diff(diff)

    hunk = files[0].hunks[0]
    assert (hunk.old_start, hunk.old_count) == (3, 1)
    assert (hunk.new_start, hunk.new_count) == (3, 1)


def test_added_and_deleted_files_are_classified() -> None:
    diff = (
        "diff --git a/added.py b/added.py\n"
        "new file mode 100644\n"
        "--- /dev/null\n"
        "+++ b/added.py\n"
        "@@ -0,0 +1,2 @@\n"
        "+first\n"
        "+second\n"
        "diff --git a/gone.py b/gone.py\n"
        "deleted file mode 100644\n"
        "--- a/gone.py\n"
        "+++ /dev/null\n"
        "@@ -1,1 +0,0 @@\n"
        "-bye\n"
    )

    files, _ = parse_unified_diff(diff)

    assert [(item.path, item.change) for item in files] == [
        ("added.py", "added"),
        ("gone.py", "deleted"),
    ]
    assert files[0].additions == 2
    assert files[1].deletions == 1


def test_a_rename_keeps_both_paths() -> None:
    diff = (
        "diff --git a/old/name.py b/new/name.py\n"
        "similarity index 96%\n"
        "rename from old/name.py\n"
        "rename to new/name.py\n"
        "--- a/old/name.py\n"
        "+++ b/new/name.py\n"
        "@@ -1,2 +1,2 @@\n"
        " keep\n"
        "-before\n"
        "+after\n"
    )

    files, _ = parse_unified_diff(diff)

    assert files[0].path == "new/name.py"
    assert files[0].old_path == "old/name.py"
    assert files[0].change == "renamed"


def test_a_binary_file_is_flagged_rather_than_parsed() -> None:
    diff = (
        "diff --git a/logo.png b/logo.png\n"
        "index 3333333..4444444 100644\n"
        "Binary files a/logo.png and b/logo.png differ\n"
    )

    files, _ = parse_unified_diff(diff)

    assert files[0].binary is True
    assert files[0].hunks == ()


def test_the_no_newline_marker_is_not_counted_as_a_line() -> None:
    diff = (
        "diff --git a/tail.txt b/tail.txt\n"
        "--- a/tail.txt\n"
        "+++ b/tail.txt\n"
        "@@ -1,1 +1,1 @@\n"
        "-old\n"
        "\\ No newline at end of file\n"
        "+new\n"
        "\\ No newline at end of file\n"
    )

    files, _ = parse_unified_diff(diff)

    assert (files[0].additions, files[0].deletions) == (1, 1)
    assert len(files[0].hunks[0].lines) == 2


def test_an_oversized_patch_is_truncated_rather_than_returned_whole() -> None:
    body = "".join(
        f"diff --git a/f{index}.txt b/f{index}.txt\n"
        f"--- a/f{index}.txt\n"
        f"+++ b/f{index}.txt\n"
        "@@ -1,1 +1,1 @@\n"
        "-a\n"
        "+b\n"
        for index in range(MAX_PATCH_FILES + 5)
    )

    files, truncated = parse_unified_diff(body)

    assert truncated is True
    assert len(files) == MAX_PATCH_FILES


def test_an_empty_diff_produces_no_files() -> None:
    files, truncated = parse_unified_diff("")

    assert files == ()
    assert truncated is False
