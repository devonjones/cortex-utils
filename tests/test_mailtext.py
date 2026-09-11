"""Tests for email body -> readable text."""

import concurrent.futures

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
        assert to_text("Science: 78%", "<p>something else</p>") == "Science: 78%"

    def test_falls_back_to_html_when_plain_is_empty(self):
        # The 71% case: body_text empty, body_html populated.
        assert "504 consent form" in to_text("", EMAIL_HTML)
        assert "504 consent form" in to_text(None, EMAIL_HTML)

    def test_whitespace_only_plain_counts_as_empty(self):
        assert "504 consent form" in to_text("   \n\t ", EMAIL_HTML)

    def test_mislabelled_html_in_the_plain_part_is_converted(self):
        # Otherwise raw markup reaches the reader.
        out = to_text(EMAIL_HTML, None)
        assert "504 consent form" in out
        assert "<table" not in out

    def test_nothing_usable_returns_empty_string(self):
        assert to_text(None, None) == ""
        assert to_text("", "") == ""

    def test_concurrent_calls_are_safe(self):
        """A shared HTML2Text instance is not usable from two threads at once.

        html2text's finish() clears its output buffer per call, so *sequential*
        reuse is fine -- which is why the module-level singleton this code was
        lifted from looked correct. Under real concurrency it is not: two
        threads feed the same HTMLParser rawdata buffer, and the failure is not
        a subtle interleave but an AssertionError raised from inside
        html.parser.

        Documents here are deliberately large. Short ones complete inside a
        single GIL slice and never preempt, so a small-input version of this
        test passes against the singleton and proves nothing.
        """
        docs = ["<html><body>" + f"<p>m{i}</p>" * 2000 + "</body></html>" for i in range(24)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda d: to_text(None, d), docs))

        for i, out in enumerate(results):
            assert set(out.split()) == {f"m{i}"}, f"output {i} was contaminated"
