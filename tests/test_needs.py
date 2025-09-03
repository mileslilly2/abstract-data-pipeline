from adp.core.base import Source, Transform, Sink, Context, Record
from typing import Iterator, Iterable

class DummySource(Source):
    def run(self, ctx: Context) -> Iterator[Record]:
        yield {"text": "hello"}
        yield {"text": "world"}

class DummyUppercase(Transform):
    def run(self, ctx: Context, rows: Iterable[Record]) -> Iterator[Record]:
        for r in rows:
            yield {"text": r["text"].upper()}

class DummySink(Sink):
    def run(self, ctx: Context, rows: Iterable[Record]):
        for r in rows:
            print("SINK OUT:", r)
        return f"{len(list(rows))} rows printed"
