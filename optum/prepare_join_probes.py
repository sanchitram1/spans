import argparse
import copy
import json
from pathlib import Path

parser = argparse.ArgumentParser(description='Prepare count probes from a captured native GetSessions selection query.')
parser.add_argument('selection', type=Path)
parser.add_argument('output', type=Path)
args = parser.parse_args()
query = json.loads(args.selection.read_text())
if query.get('queryType') != 'topN' or query.get('dataSource', {}).get('type') != 'join':
    parser.error('expected a native topN query with a join datasource')
args.output.mkdir(parents=True, exist_ok=False)
count = {'type': 'count', 'name': '__joined_rows'}
if any(a.get('name') == count['name'] for a in query.get('aggregations', [])):
    parser.error('reserved count aggregator already exists')
counted_selection = copy.deepcopy(query)
counted_selection.setdefault('aggregations', []).append(count)
base = {k: copy.deepcopy(query[k]) for k in
        ['dataSource', 'intervals', 'filter', 'virtualColumns', 'granularity', 'context'] if k in query}
base.setdefault('context', {}).update(useCache=False, populateCache=False, timeout=60000)
base['context'].pop('queryId', None)
probes = {
    'selection-with-counts': counted_selection,
    'joined-rows-total': dict(base, queryType='timeseries', granularity='all', aggregations=[count]),
    'joined-rows-by-trace': dict(base, queryType='groupBy', granularity='all',
                               dimensions=['context.trace_id'], aggregations=[count]),
}
right = query['dataSource']['right']
if right.get('type') == 'query':
    probes['right-input'] = right['query']
for name, probe in probes.items():
    probe.setdefault('context', {}).update(useCache=False, populateCache=False, timeout=60000,
                                          queryId='optum-fanout-' + name)
    (args.output / (name + '.json')).write_text(json.dumps(probe, indent=2) + '\n')
print('Prepared:', ', '.join(probes))
