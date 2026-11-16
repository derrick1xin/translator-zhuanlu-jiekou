from translation_stream import (
    SentenceEmitter,
    TextAccumulator,
    extract_text_chunk,
    parse_sse_events,
    score_translation,
)


def test_sse_events_join_data_lines_and_flush_eof():
    lines = ["event: message\n", "data: first\n", "data: second\n", "\n", "data: tail"]
    assert list(parse_sse_events(lines)) == ["first\nsecond", "tail"]


def test_extract_plain_text_and_nested_delta():
    plain = extract_text_chunk("Hello")
    nested = extract_text_chunk('{"choices":[{"delta":{"content":" world"}}]}')
    assert plain is not None and plain.text == "Hello" and plain.kind == "delta"
    assert nested is not None and nested.text == " world" and nested.kind == "delta"


def test_snapshot_is_not_duplicated():
    accumulator = TextAccumulator()
    assert accumulator.add("Hello", kind="snapshot") == "Hello"
    assert accumulator.add("Hello world", kind="snapshot") == " world"
    assert accumulator.add("Hello world", kind="snapshot") == ""
    assert accumulator.text == "Hello world"


def test_delta_preserves_legitimate_repeated_words():
    accumulator = TextAccumulator()
    accumulator.add("very ")
    accumulator.add("very good")
    assert accumulator.text == "very very good"


def test_sentence_emitter_flushes_complete_and_unterminated_sentences():
    emitter = SentenceEmitter()
    assert emitter.feed("Ready when you are. Next") == ["Ready when you are."]
    assert emitter.feed(" step！") == ["Next step！"]
    assert emitter.feed("tail", final=True) == ["tail"]


def test_done_payload_is_not_text():
    assert extract_text_chunk(" [DONE] ") is None


def test_exact_translation_scores_100():
    scores = score_translation("Hello world", "Hello world")
    assert scores == {"bleu": 100.0, "chrf": 100.0, "composite": 100.0}


def test_empty_candidate_scores_zero():
    scores = score_translation("", "Hello world")
    assert scores == {"bleu": 0.0, "chrf": 0.0, "composite": 0.0}
