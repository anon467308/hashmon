#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt

raw = pd.read_csv('accuracy.csv', sep=';', header=0,
        dtype={'formula': str, 'bits': int, 'fpl': float, 'fpu': float,
            'fnl': float, 'fnu': float, 'dl': float, 'du': float,
            'fpb': float, 'fnb': float, 'db': float})
raw.set_index(['formula', 'bits'], inplace=True)

observed = (raw.loc[:, 'dl'] + raw.loc[:, 'du']) / 2  # mid-point of confidence interval
bounds = raw.loc[:, 'db']

plt.figure(figsize=(10,5), dpi=100)
plt.style.use('seaborn-colorblind')
styles = {'ex1': 's-', 'frd': 's-', 'e1': 'o-', 'e2': 'o-', 'e3': '^-', 'e3_let': '^:', 'e4': '^:'}

plt.subplot(1, 2, 1)
for formula, df in observed.groupby(level=0):
    x = df.index.get_level_values(1)
    plt.plot(x, df, styles[formula], label=formula, linewidth=2, markersize=6)
    plt.xticks(x)
plt.grid()
plt.title('Observed error probability')
plt.xlabel('Number of hash bits')
plt.ylim(-0.05, 1.05)
plt.legend()

plt.subplot(1, 2, 2)
for formula, df in bounds.groupby(level=0):
    x = df.index.get_level_values(1)
    plt.plot(x, df, styles[formula], label=formula, linewidth=2, markersize=6)
    plt.xticks(x)
plt.grid()
plt.title('Computed error bound')
plt.xlabel('Number of hash bits')
plt.ylim(-0.05, 1.05)
plt.legend()

plt.tight_layout()
plt.savefig('accuracy.png')
