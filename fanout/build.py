import argparse
import collections
import datetime
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spans.config import Config, GENERATOR_VERSION
from spans.generate import Generator
from spans.ingest import load_fixture
from spans.otlp import request_line, string_attr

SOURCE = Path(__file__).parent / 'profile.json'
parser = argparse.ArgumentParser()
parser.add_argument('output', type=Path)
OUTPUT = parser.parse_args().output
START = datetime.datetime(2026, 11, 6, tzinfo=datetime.UTC)
DAY_NS = 86_400_000_000_000


def ident(*parts, size=32):
    return hashlib.sha256(':'.join(map(str, parts)).encode()).hexdigest()[:size]


if OUTPUT.exists():
    raise SystemExit(f'Refusing to overwrite {OUTPUT}')
OUTPUT.mkdir()
profile = json.loads(SOURCE.read_text())
all_spans, all_rows, components = [], [], []
for day in range(2):
    for index, shape in enumerate(profile):
        count, roots = shape['spans'], shape['roots']
        sessions = max(1, roots)
        config = Config(
            number_of_spans=count, number_of_traces=1,
            min_spans_per_trace=count, max_spans_per_trace=count,
            min_roots_per_trace=max(1, roots), max_roots_per_trace=max(1, roots),
            session_trace_coverage=1, min_traces_per_session=1, max_traces_per_session=1,
            sessions_per_trace=sessions, session_id_missing_fraction=0,
            min_text_bytes=128, max_text_bytes=512,
            start=(START + datetime.timedelta(days=day)).isoformat(), days=1,
            project='fanout', seed=42000 + day * len(profile) + index, batch_spans=1000,
        )
        generator = Generator(config)
        spans = [span for line, _ in generator for rs in json.loads(line)['resourceSpans']
                 for scope in rs['scopeSpans'] for span in scope['spans']]
        rows = [dict(zip(generator.parquet_data, values))
                for values in zip(*generator.parquet_data.values())]
        trace_id = ident('fanout', 'trace', index)
        candidates = collections.defaultdict(list)
        for i, span in enumerate(spans):
            if span.get('parentSpanId'):
                candidates[rows[i]['session_id']].append(i)
        chosen = {positions[0] for positions in candidates.values()}
        remaining = sorted((i for positions in candidates.values() for i in positions if i not in chosen),
                           key=lambda i: ident('tag', spans[i]['spanId']))
        chosen.update(remaining[:shape['tagged'] - len(chosen)])
        shifts = {}
        root_spans = [s for s in spans if not s.get('parentSpanId')]
        for root_index, span in enumerate(root_spans):
            target = int((START + datetime.timedelta(days=day)).timestamp()) * 10**9
            target += (root_index + 1) * (DAY_NS - 60_000_000_000) // (len(root_spans) + 1)
            shifts[span['spanId']] = target - int(span['startTimeUnixNano'])
        for i, (span, row) in enumerate(zip(spans, rows)):
            span['traceId'] = row['trace_id'] = trace_id
            root_id = span.get('parentSpanId', span['spanId'])
            shift = shifts[root_id]
            span['startTimeUnixNano'] = str(int(span['startTimeUnixNano']) + shift)
            span['endTimeUnixNano'] = str(int(span['endTimeUnixNano']) + shift)
            row['start_time'] = int(span['startTimeUnixNano'])
            span['attributes'] = [a for a in span['attributes'] if a['key'] != 'session.id']
            row['span_session_id'] = row['session_id'] if i in chosen else None
            if i in chosen:
                span['attributes'].append(string_attr('session.id', row['session_id']))
            if roots == 0 and not span.get('parentSpanId'):
                span['parentSpanId'] = row['parent_span_id'] = ident('missing-parent', day, index, size=16)
        all_spans.extend(spans)
        all_rows.extend(rows)
        components.append({'day': day, 'trace_id': trace_id, 'spans': count,
                           'roots': roots, 'sessions': sessions, 'tagged': len(chosen),
                           'generator_config': config.content()})
    print(f'Generated day {day + 1}: {len(all_spans)} total spans', flush=True)

order = sorted(range(len(all_spans)), key=lambda i: (int(all_spans[i]['startTimeUnixNano']), all_spans[i]['spanId']))
files = []
for batch, offset in enumerate(range(0, len(order), 1000)):
    payload = request_line([all_spans[i] for i in order[offset:offset + 1000]], 'fanout').encode()
    name = f'traces-{batch:06d}.jsonl'
    (OUTPUT / name).write_bytes(payload)
    files.append({'path': name, 'spans': min(1000, len(order) - offset),
                  'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()})
columns = {k: [all_rows[i][k] for i in order] for k in all_rows[0]}
columns['start_time'] = pa.array(columns['start_time'], type=pa.timestamp('ns', tz='UTC'))
pq.write_table(pa.table(columns), OUTPUT / 'spans.parquet')
parquet = (OUTPUT / 'spans.parquet').read_bytes()
manifest = {'generator_version': GENERATOR_VERSION, 'seed': 42000,
            'config': {'project': 'fanout', 'start': START.isoformat(), 'days': 2,
                       'number_of_spans': len(order), 'number_of_traces': len(profile),
                       'composition': 'two repetitions of sampled Nov4 trace sizes; synthetic IDs'},
            'batch_spans': 1000, 'files': files, 'total_bytes': sum(f['bytes'] for f in files),
            'parquet': {'path': 'spans.parquet', 'bytes': len(parquet),
                        'sha256': hashlib.sha256(parquet).hexdigest()},
            'components': components}
(OUTPUT / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
counts = collections.Counter(row['trace_id'] for row in all_rows)
tagged = collections.Counter(row['trace_id'] for row in all_rows if row['span_session_id'])
summary = {'spans': len(order), 'traces': len(counts),
           'sessions': len({row['span_session_id'] for row in all_rows if row['span_session_id']}),
           'roots': sum(row['parent_span_id'] is None for row in all_rows),
           'tagged_spans': sum(tagged.values()),
           'tagged_roots': sum(row['span_session_id'] is not None and row['parent_span_id'] is None for row in all_rows),
           'unique_span_ids': len({row['span_id'] for row in all_rows}),
           'all_row_join_pairs': sum(n*n for n in counts.values()),
           'tagged_left_join_pairs': sum(n*tagged[t] for t,n in counts.items()),
           'otlp_bytes': manifest['total_bytes'], 'parquet_bytes': len(parquet)}
(OUTPUT / 'topology.json').write_text(json.dumps(summary, indent=2) + '\n')
fixture = load_fixture(OUTPUT)
assert fixture.total_spans == 75178
assert summary['unique_span_ids'] == summary['spans']
assert summary['tagged_spans'] == 12132 and summary['tagged_roots'] == 0
print(json.dumps(summary, indent=2))
