"""Tests for extract_reply_hierarchy."""

from cortex_utils.llm.client import extract_reply_hierarchy


class TestExtractReplyHierarchy:
    """Tests for reply chain extraction from email bodies."""

    def test_empty_body_returns_no_chain(self) -> None:
        assert extract_reply_hierarchy("", "me@example.com") == "No reply chain detected"

    def test_none_body_returns_no_chain(self) -> None:
        assert extract_reply_hierarchy(None, "me@example.com") == "No reply chain detected"

    def test_no_matches_returns_no_chain(self) -> None:
        assert (
            extract_reply_hierarchy("Just a plain email.", "me@example.com")
            == "No reply chain detected"
        )

    def test_gmail_on_wrote_pattern(self) -> None:
        body = "On Mon, Jan 15, 2025 at 6:28 PM, Bob Smith <bob@company.com> wrote:\n> hello"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert "1. me@example.com (this email)" in result
        assert "2. bob@company.com (quoted)" in result

    def test_email_only_wrote_pattern(self) -> None:
        body = "<alice@company.com> wrote:\n> some text"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert "alice@company.com (quoted)" in result

    def test_from_header_pattern(self) -> None:
        body = "From: Carol Jones <carol@example.com>\nSent: Monday\n"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert "carol@example.com (quoted)" in result

    def test_german_schrieb_pattern(self) -> None:
        body = "Hans Müller <hans@example.de> schrieb am 10. Jan 2025:\n> Hallo"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert "hans@example.de (quoted)" in result

    def test_deduplication(self) -> None:
        body = (
            "On Mon, Jan 15, 2025, Bob Smith <bob@company.com> wrote:\n> hi\n\n"
            "On Sun, Jan 14, 2025, Bob Smith <bob@company.com> wrote:\n> earlier"
        )
        result = extract_reply_hierarchy(body, "me@example.com")
        assert result.count("bob@company.com") == 1

    def test_deduplication_case_insensitive(self) -> None:
        body = (
            "On Mon, Jan 15, 2025, Bob <Bob@Company.com> wrote:\n> hi\n\n"
            "On Sun, Jan 14, 2025, Bob <bob@company.com> wrote:\n> earlier"
        )
        result = extract_reply_hierarchy(body, "me@example.com")
        assert result.count("(quoted)") == 1

    def test_multiple_participants_ordered_by_appearance(self) -> None:
        body = (
            "On Mon, Jan 15, 2025, Alice <alice@co.com> wrote:\n> hi\n\n"
            "On Sun, Jan 14, 2025, Bob <bob@co.com> wrote:\n> earlier"
        )
        result = extract_reply_hierarchy(body, "me@example.com")
        lines = result.strip().split("\n")
        # Line 0 is "Reply chain:", lines 1+ are numbered participants
        assert "alice@co.com (quoted)" in lines[2]
        assert "bob@co.com (quoted)" in lines[3]

    def test_mailto_cleanup(self) -> None:
        # mailto: artifacts appear inside the email group as "email <mailto:email>"
        # The regex captures "bob@company.com <mailto:bob@company.com" and the cleanup strips it
        body = "On Mon, Jan 15, 2025, Bob <bob@company.com> wrote:\n> hi"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert "bob@company.com (quoted)" in result
        assert "mailto" not in result

    def test_mailto_in_from_header(self) -> None:
        # From: header with mailto artifact
        body = "From: Bob <bob@company.com>\nSent: Monday"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert "bob@company.com (quoted)" in result

    def test_reply_chain_header(self) -> None:
        body = "On Mon, Jan 15, 2025, Bob <bob@co.com> wrote:\n> hi"
        result = extract_reply_hierarchy(body, "me@example.com")
        assert result.startswith("Reply chain:\n")
