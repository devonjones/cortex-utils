"""Tests for email body -> readable text."""

import concurrent.futures

import pytest

from cortex_utils.mailtext import html_to_markdown, looks_like_html, to_text

EMAIL_HTML = """
<html><head><title>t</title></head><body>
<table cellpadding="0" cellspacing="0"><tr><td style="color:#333">
<div class="wrap"><p>Please sign and return the <b>504 consent form</b>.</p>
<p><a href="https://example.org/form">Open the form</a></p></div>
</td></tr></table></body></html>
"""


class TestHtmlToMarkdown:
    def test_keeps_links_and_emphasis_drops_images(self):
        out = html_to_markdown(
            '<p>See <a href="https://x.test/a">here</a> <b>now</b>'
            '<img src="https://x.test/pixel.gif"></p>'
        )
        assert "[here](https://x.test/a)" in out
        assert "**now**" in out
        assert "pixel.gif" not in out

    def test_does_not_wrap_lines(self):
        long_sentence = "word " * 100
        assert "\n" not in html_to_markdown(f"<p>{long_sentence}</p>").strip()

    def test_empty_input_is_empty_output(self):
        assert html_to_markdown("") == ""


class TestLooksLikeHtml:
    def test_detects_mislabelled_email_html(self):
        assert looks_like_html(EMAIL_HTML)

    def test_plain_text_is_not_html(self):
        assert not looks_like_html(
            "Dear Mom and Dad,\n\nI looked up my grades today. Science: 78%.\n\n-Aurelia"
        )

    def test_too_short_to_judge(self):
        assert not looks_like_html("<html><body><div><p>hi</p></div></body></html>"[:49])

    def test_no_angle_brackets_short_circuits(self):
        assert not looks_like_html("a plain sentence repeated. " * 10)

    def test_xml_json_and_templates_are_excluded(self):
        # A false positive mangles genuinely plain text, so these bail out
        # before the tag count is even considered.
        assert not looks_like_html('<?xml version="1.0"?><a><b/><c/><d/><e/><f/></a>' * 3)
        assert not looks_like_html('{"key": "<div><p><span><b><i>value"}' * 5)
        assert not looks_like_html("{{ tpl }}<div><p><span><b><i>x</i></b></span></p></div>" * 3)

    def test_markup_without_email_indicators_is_not_enough(self):
        # Five tags, but none of them email-shaped.
        assert not looks_like_html("<aa><bb><cc><dd><ee>" + ("filler text " * 20))

    def test_only_first_8kb_is_sampled(self):
        # Long tracking preamble then real HTML: the tail is not inspected.
        assert not looks_like_html("https://track.test/x?q=1 " * 500 + EMAIL_HTML)


class TestToText:
    def test_prefers_a_real_plain_part(self):
        assert (
            to_text(body_text="Science: 78%", body_html="<p>something else</p>") == "Science: 78%"
        )

    def test_falls_back_to_html_when_plain_is_empty(self):
        # The 71% case: body_text empty, body_html populated.
        assert "504 consent form" in to_text(body_text="", body_html=EMAIL_HTML)
        assert "504 consent form" in to_text(body_text=None, body_html=EMAIL_HTML)

    def test_whitespace_only_plain_counts_as_empty(self):
        assert "504 consent form" in to_text(body_text="   \n\t ", body_html=EMAIL_HTML)

    def test_mislabelled_html_in_the_plain_part_is_converted(self):
        # Otherwise raw markup reaches the reader.
        out = to_text(body_text=EMAIL_HTML, body_html=None)
        assert "504 consent form" in out
        assert "<table" not in out

    def test_nothing_usable_returns_empty_string(self):
        assert to_text(body_text=None, body_html=None) == ""
        assert to_text(body_text="", body_html="") == ""

    def test_concurrent_calls_are_safe(self):
        """A shared HTML2Text instance is not usable from two threads at once.

        html2text's finish() clears its output buffer per call, so *sequential*
        reuse is fine -- which is why the module-level singleton this code was
        lifted from looked correct. Under real concurrency it is not: two
        threads feed the same HTMLParser rawdata buffer. It fails two ways: most
        often an AssertionError from inside html.parser, but in every trial
        some documents also came back with NO exception and words from two
        different messages mixed together -- silent corruption is the reason
        this matters, not the loud crash.

        Documents here are deliberately large. Short ones complete inside a
        single GIL slice and never preempt, so a small-input version of this
        test passes against the singleton and proves nothing.
        """
        docs = ["<html><body>" + f"<p>m{i}</p>" * 2000 + "</body></html>" for i in range(24)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda d: to_text(body_text=None, body_html=d), docs))

        for i, out in enumerate(results):
            assert set(out.split()) == {f"m{i}"}, f"output {i} was contaminated"


class TestBranchesReviewFlagged:
    """Branches that no earlier test exercised — deleting them passed the suite."""

    def test_too_few_tags_is_not_html(self):
        # 2 tags and 2 email indicators, but under the 5-tag floor. Without
        # this, removing the tag-count check broke nothing.
        assert not looks_like_html("<div><table>" + ("some ordinary prose here " * 5))

    def test_exactly_five_tags_is_the_boundary(self):
        four = "<div><table><tr><td>" + ("filler text " * 10)
        five = "<div><table><tr><td><body>" + ("filler text " * 10)
        assert not looks_like_html(four)
        assert looks_like_html(five)

    def test_plain_part_converting_to_nothing_falls_through_to_html(self):
        # A mislabelled text/plain carrying only a tracking pixel converts to
        # "" — that is not a failure, so the html part should still be tried.
        pixel = (
            '<html><body><div><table><tr><td><img src="https://t.test/p.gif">'
            + ("<!-- spacer -->" * 6)
            + "</td></tr></table></div></body></html>"
        )
        # Faithful conversion yields markdown punctuation, not nothing.
        assert html_to_markdown(pixel) == "---"
        assert "504 consent form" in to_text(body_text=pixel, body_html=EMAIL_HTML)


class TestEntitiesAreNotTransliterated:
    """html2text's default rewrites entities to ASCII. Names are not disposable."""

    def test_accented_names_survive_as_entities(self):
        out = to_text(body_text=None, body_html="<p>Jos&#233; Mu&ntilde;oz, Zo&euml;</p>")
        assert "José" in out and "Muñoz" in out and "Zoë" in out

    def test_spanish_language_mail_survives(self):
        # DPS mails families in Spanish; "Reunión" must not arrive as "Reunion".
        out = to_text(body_text=None, body_html="<p>Reuni&oacute;n el mi&eacute;rcoles</p>")
        assert out == "Reunión el miércoles"

    def test_literal_utf8_still_survives(self):
        assert to_text(body_text=None, body_html="<p>Reunión el miércoles</p>") == (
            "Reunión el miércoles"
        )

    def test_nbsp_normalised_to_a_plain_space(self):
        # unicode_snob keeps U+00A0; we normalise it so it does not read as a
        # stray character in a digest.
        out = to_text(body_text=None, body_html="<p>Room&nbsp;204&nbsp;at&nbsp;8:15</p>")
        assert out == "Room 204 at 8:15"
        assert "\xa0" not in out


class TestConversionFailureIsSurvivable:
    """postmark wrapped every call site; the lift must not drop that."""

    def test_failure_falls_back_to_raw_text(self, monkeypatch):
        def boom(_):
            raise AssertionError("html.parser blew up")

        monkeypatch.setattr("cortex_utils.mailtext.html_to_markdown", boom)
        assert to_text(body_text=EMAIL_HTML, body_html=None) == EMAIL_HTML.strip()

    def test_failure_on_html_part_keeps_what_the_text_part_left(self, monkeypatch):
        # No salvage path any more: the log line carries the failure signal and
        # there is nothing better to return than whatever body_text gave.
        # Mislabelled text/plain that converts to symbols only (a tick grid).
        pixel = (
            "<html><body><div><table><tr><td>&#10003; &#10003;</td></tr>"
            "</table></div></body></html>"
        )
        real = html_to_markdown

        def only_html_fails(content):
            if content == EMAIL_HTML:
                raise ValueError("nope")
            return real(content)

        monkeypatch.setattr("cortex_utils.mailtext.html_to_markdown", only_html_fails)
        out = to_text(body_text=pixel, body_html=EMAIL_HTML)
        assert "✓ ✓" in out and out != ""

    def test_failure_on_both_parts_returns_the_raw_text(self, monkeypatch):
        monkeypatch.setattr(
            "cortex_utils.mailtext.html_to_markdown",
            lambda _: (_ for _ in ()).throw(ValueError("nope")),
        )
        assert to_text(body_text=EMAIL_HTML, body_html="<p>x</p>") == EMAIL_HTML.strip()

    def test_text_part_failure_still_lets_html_part_try(self, monkeypatch):
        # code-reviewer round 3: an early return here skipped a perfectly good
        # body_html whenever the mislabelled text part would not convert.
        real = html_to_markdown

        def only_text_fails(content):
            if content == "<div><table><tr><td><body>bad markup here</td></tr></table></div>":
                raise ValueError("nope")
            return real(content)

        monkeypatch.setattr("cortex_utils.mailtext.html_to_markdown", only_text_fails)
        bad = "<div><table><tr><td><body>bad markup here</td></tr></table></div>"
        assert "504 consent form" in to_text(body_text=bad, body_html=EMAIL_HTML)

    def test_html_to_markdown_itself_still_raises(self, monkeypatch):
        # The low-level function stays honest; to_text is the forgiving one.
        import cortex_utils.mailtext as m

        monkeypatch.setattr(m, "_converter", lambda: (_ for _ in ()).throw(RuntimeError("x")))
        with pytest.raises(RuntimeError):
            m.html_to_markdown("<p>hi</p>")


class TestSymbolOnlyBodiesAreNotDropped:
    """_has_content gates fallthrough, not returnability."""

    def test_symbol_only_html_is_returned_when_nothing_else_exists(self):
        # A checkmark attendance grid is that message's content. Dropping it
        # because it has no alphanumerics would lose the whole body.
        out = to_text(body_text=None, body_html="<p>&#10003; &#10003; &#10007; &#10003;</p>")
        assert out == "✓ ✓ ✗ ✓"

    def test_symbol_only_still_loses_to_a_part_with_real_content(self):
        pixel = (
            '<html><body><div><table><tr><td><img src="https://t.test/p.gif">'
            + ("<!-- spacer -->" * 6)
            + "</td></tr></table></div></body></html>"
        )
        assert "504 consent form" in to_text(body_text=pixel, body_html=EMAIL_HTML)

    def test_html_part_converting_to_punctuation_is_returned_as_last_resort(self):
        # test-coverage-reviewer proved deleting the body_html _has_content
        # check left every test green; this pins the branch.
        pixel = (
            '<html><body><div><table><tr><td><img src="https://t.test/p.gif">'
            + ("<!-- spacer -->" * 6)
            + "</td></tr></table></div></body></html>"
        )
        assert html_to_markdown(pixel) == "---"
        assert to_text(body_text=None, body_html=pixel) == "---"

    def test_truly_nothing_is_still_empty(self):
        assert to_text(body_text=None, body_html="<html><body></body></html>") == ""


class TestFallbackPrecedence:
    """Two content-free parts: keep the one carrying more, not the first seen."""

    def test_richer_content_free_part_wins_regardless_of_order(self):
        # body_text must look like HTML to be converted at all; body_html is
        # converted unconditionally, so it can be minimal.
        rule = "<html><body><div><table><tr><td>-</td></tr></table></div></body></html>"
        ticks = "<p>&#10003; &#10003; &#10007; &#10003; &#10003;</p>"
        assert to_text(body_text=rule, body_html=ticks) == "✓ ✓ ✗ ✓ ✓"

    def test_equal_length_tie_goes_to_the_text_part(self):
        # `>` not `>=`: on a tie the earlier (text/plain) candidate holds, since
        # text/plain is the sender's own rendering. Both sides here convert to
        # exactly 7 characters -- an unequal pair would not exercise the tie at
        # all, which is how the first version of this test passed while the
        # `>=` mutant survived it.
        rule = "<html><body><div><table><tr><td>-</td></tr></table></div></body></html>"
        crosses = "<p>" + "&#10007;" * 7 + "</p>"
        assert len(html_to_markdown(rule)) == len(html_to_markdown(crosses)) == 7
        assert to_text(body_text=rule, body_html=crosses) == html_to_markdown(rule)
