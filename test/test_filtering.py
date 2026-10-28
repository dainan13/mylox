"""Table routing rules (evaluation issue P1-8)."""

from __future__ import annotations

from mylox.filtering import FilterMode, TableFilter, coerce_filter


def test_default_allow_everything():
    table_filter = TableFilter([])
    assert table_filter.is_allowed("any", "table") is True


def test_gitignore_last_match_wins_and_negation_reincludes():
    table_filter = TableFilter(["*.log", "!important.log"])
    assert table_filter.is_allowed("db", "debug.log") is False
    assert table_filter.is_allowed("db", "important.log") is True
    assert table_filter.is_allowed("db", "orders") is True


def test_gitignore_full_name_and_table_name_both_match():
    table_filter = TableFilter(["arch2018.*"])
    assert table_filter.is_allowed("arch2018", "t1") is False
    assert table_filter.is_allowed("other", "t1") is True
    # a bare table pattern matches in every schema
    assert TableFilter(["sys_log"]).is_allowed("any", "sys_log") is False


def test_include_mode_keeps_legacy_behaviour():
    table_filter = TableFilter(["cinema_base.*", "!cinema_base.sys_log"], mode="include")
    assert table_filter.is_allowed("cinema_base", "t_hall") is True
    assert table_filter.is_allowed("cinema_base", "sys_log") is False
    assert table_filter.is_allowed("other", "t_hall") is False
    # no rules at all -> everything allowed
    assert TableFilter([], mode=FilterMode.INCLUDE).is_allowed("x", "y") is True


def test_decision_reports_the_matching_rule():
    table_filter = TableFilter(["*.log", "!keep.log"])
    allowed, rule = table_filter.decision("db", "keep.log")
    assert allowed is True and rule == "!keep.log"
    allowed, rule = table_filter.decision("db", "x.log")
    assert allowed is False and rule == "*.log"
    allowed, rule = table_filter.decision("db", "orders")
    assert allowed is True and rule is None


def test_blank_and_comment_rules_are_ignored():
    table_filter = TableFilter(["", "   ", "# comment", "real"])
    assert table_filter.rules == ("real",)


def test_coerce_filter_accepts_str_list_and_instance():
    assert coerce_filter(None).is_allowed("a", "b") is True
    assert coerce_filter("a.*").is_allowed("a", "b") is False
    existing = TableFilter(["a.*"])
    assert coerce_filter(existing) is existing


def test_gitignore_include_form_and_include_mode_are_equivalent_for_one_db():
    """The two dialects that express "only this schema" must agree.

    ``["db.*"]`` means the *opposite* depending on the mode, which is exactly the
    trap worth documenting: in gitignore mode it excludes the schema.
    """
    gitignore_only_db = TableFilter(["*", "!db.*"])
    include_only_db = TableFilter(["db.*"], mode="include")
    for db, table in (("db", "t1"), ("other", "t1"), ("db", "sys_log")):
        assert gitignore_only_db.is_allowed(db, table) == include_only_db.is_allowed(db, table), (db, table)
    assert gitignore_only_db.is_allowed("db", "t1") is True
    assert gitignore_only_db.is_allowed("other", "t1") is False

    # the plain pattern is the trap: exclude in gitignore mode, include otherwise
    assert TableFilter(["db.*"]).is_allowed("db", "t1") is False
    assert TableFilter(["db.*"], mode="include").is_allowed("db", "t1") is True
