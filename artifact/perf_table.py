#!/usr/bin/env python3

import pandas as pd

expkey = ['type', 'formula', 'rate', 'strlen']
modetype = pd.CategoricalDtype(['dejavu', 'bypass', 'off', 'single', 'merged'])
raw = pd.read_csv('perf.csv', sep=';', header=0,
        dtype={'formula': str, 'type': str, 'rate': float, 'strlen': int,
            'mode': modetype, 'rep': int, 'time': float, 'memory': float, 'fp': float, 'fn': float},
        index_col=expkey + ['mode', 'rep'])

aggregated = raw.groupby(expkey + ['mode'], observed=True).agg(
        {'time': 'mean', 'memory': 'mean', 'fp': 'max', 'fn': 'max'})

aggregated['time_ratio'] = aggregated['time'] / aggregated.xs('bypass', level='mode')['time'] - 1
aggregated['memory_ratio'] = aggregated['memory'] / aggregated.xs('bypass', level='mode')['memory'] - 1
aggregated = aggregated[['memory', 'memory_ratio', 'time', 'time_ratio', 'fp', 'fn']]

reshaped = aggregated.unstack('mode')
reshaped.drop([('memory_ratio', 'bypass'), ('time_ratio', 'bypass'),
    ('fp', 'dejavu'), ('fp', 'bypass'), ('fp', 'off'),
    ('fn', 'dejavu'), ('fn', 'bypass'), ('fn', 'off'),], axis='columns', inplace=True)

memoryfmt = lambda x: '{:.1f}'.format(x/1024) # MiB
timefmt = lambda x: '{:.1f}'.format(x)
ratiofmt = lambda x: '{:.0f}%'.format(x*100)
probfmt = lambda x: '{:.0e}'.format(x)

print(reshaped.to_string(na_rep='n/a', formatters={
    ('memory', 'dejavu'): memoryfmt,
    ('memory', 'bypass'): memoryfmt,
    ('memory', 'off'): memoryfmt,
    ('memory', 'single'): memoryfmt,
    ('memory', 'merged'): memoryfmt,
    ('memory_ratio', 'dejavu'): ratiofmt,
    ('memory_ratio', 'off'): ratiofmt,
    ('memory_ratio', 'single'): ratiofmt,
    ('memory_ratio', 'merged'): ratiofmt,

    ('time', 'dejavu'): timefmt,
    ('time', 'bypass'): timefmt,
    ('time', 'off'): timefmt,
    ('time', 'single'): timefmt,
    ('time', 'merged'): timefmt,
    ('time_ratio', 'dejavu'): ratiofmt,
    ('time_ratio', 'off'): ratiofmt,
    ('time_ratio', 'single'): ratiofmt,
    ('time_ratio', 'merged'): ratiofmt,

    ('fp', 'single'): probfmt,
    ('fp', 'merged'): probfmt,
    ('fn', 'single'): probfmt,
    ('fn', 'merged'): probfmt,
    }))
