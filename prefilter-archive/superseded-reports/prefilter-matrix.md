# Pre-filter vs inline crossover: filter pattern x corpus size

- dim = 768, k = 10, corpus = clustered, `ef_runtime` = server default (10)
- `gain` = inline_p50 / prefilter_p50 from **serial** (single-connection) latency. > 1.0 means pre-filtering is faster. The crossover is where gain = 1.0.
- Serial latency is used to locate the inflection point because it is free of the server request-rate ceiling, and any fixed per-query overhead cancels: it adds the same constant to both paths and so cannot move the crossing point.
- `recall` = inline recall against the exact pre-filter result (pre-filter is a brute-force exact scan, so it is ground truth).
- Both paths forced via `CONFIG SET search.prefiltering-threshold-ratio` 0.0 / 1.0, so the real planner, entries fetchers and dedup all run.
- Selectivity is verified exactly per pattern with a filter-only `FT.SEARCH ... LIMIT 0 0` count.

## WARNINGS

- text AND text @ N=5000 0.100%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 0.200%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 0.400%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 0.700%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 1.000%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 2.000%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 3.500%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 5.000%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 7.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=5000 10.000%: filter violated - inline 24, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 0.100%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 0.200%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 0.400%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 0.700%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 1.000%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 2.000%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 3.500%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 5.000%: filter violated - inline 27, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 7.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=20000 10.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 0.100%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 0.200%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 0.400%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 0.700%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 1.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 2.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 3.500%: filter violated - inline 24, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 5.000%: filter violated - inline 24, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 7.000%: filter violated - inline 23, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=80000 10.000%: filter violated - inline 23, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 0.100%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 0.200%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 0.400%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 0.700%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 1.000%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 2.000%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 3.500%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 5.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 7.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=320000 10.000%: filter violated - inline 24, prefilter 0 of 30 returned docs do not match the query
- SELECTIVITY MISMATCH numeric @ 10.000%: expected 128000, got 100000
- SELECTIVITY MISMATCH tag @ 10.000%: expected 128000, got 100000
- SELECTIVITY MISMATCH text @ 10.000%: expected 128000, got 100000
- SELECTIVITY MISMATCH text OR text @ 10.000%: expected 128000, got 100000
- text AND text @ N=1280000 0.100%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 0.200%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 0.400%: filter violated - inline 26, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 0.700%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 1.000%: filter violated - inline 25, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 2.000%: filter violated - inline 24, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 3.500%: filter violated - inline 23, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 5.000%: filter violated - inline 22, prefilter 0 of 30 returned docs do not match the query
- text AND text @ N=1280000 7.000%: filter violated - inline 21, prefilter 0 of 30 returned docs do not match the query
- SELECTIVITY MISMATCH text AND text @ 10.000%: expected 128000, got 100000
- text AND text @ N=1280000 10.000%: filter violated - inline 20, prefilter 0 of 30 returned docs do not match the query
- SELECTIVITY MISMATCH num AND tag @ 10.000%: expected 128000, got 100000
- SELECTIVITY MISMATCH num AND tag AND text @ 10.000%: expected 128000, got 100000
- SELECTIVITY MISMATCH num AND NOT tag @ 10.000%: expected 128000, got 100000

## Crossover summary (selectivity where pre-filter stops winning)

Blank / `INVALID` means the inline path did not honour the filter for that pattern, so its latency is not comparable and no crossover can be read from it.

| pattern | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| numeric | 8.27% | 4.61% | 2.22% | 1.13% | 0.70% |
| tag | 5.64% | 3.45% | 1.36% | 0.65% | 0.42% |
| text | 7.57% | 4.23% | 1.85% | 0.91% | 0.56% |
| text OR text | 7.81% | 4.33% | 1.86% | 0.96% | 0.57% |
| text AND text **(inline broken)** | INVALID | INVALID | INVALID | INVALID | INVALID |
| num AND tag | 5.41% | 3.09% | 1.22% | 0.62% | 0.41% |
| num AND tag AND text | 4.98% | 2.91% | 1.40% | 0.63% | 0.36% |
| num AND NOT tag | 5.55% | 3.07% | 1.27% | 0.64% | 0.38% |

## Per-pattern tables

### numeric

Query: `@num:[0 {S}]`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 27.61x | 1.000 | 71.27x | 1.000 | 76.35x | 1.000 | 73.03x | 1.000 | 25.81x | 1.000 |
| 0.200% | 24.28x | 1.000 | 39.65x | 1.000 | 42.80x | 1.000 | 19.61x | 1.000 | 6.07x | 1.000 |
| 0.400% | 15.46x | 1.000 | 21.42x | 1.000 | 16.58x | 1.000 | 6.13x | 0.990 | 2.54x | 1.000 |
| 0.700% | 12.01x | 1.000 | 11.29x | 0.990 | 7.43x | 1.000 | 2.02x | 1.000 | 1.00x | 1.000 |
| 1.000% | 7.90x | 1.000 | 7.87x | 1.000 | 3.58x | 1.000 | 1.18x | 1.000 | 0.60x | 1.000 |
| 2.000% | 3.57x | 1.000 | 3.06x | 1.000 | 1.19x | 0.990 | 0.46x | 1.000 | 0.15x | 0.990 |
| 3.500% | 2.99x | 1.000 | 1.77x | 1.000 | 0.46x | 0.990 | 0.22x | 1.000 | 0.06x | 0.990 |
| 5.000% | 1.57x | 1.000 | 0.84x | 1.000 | 0.26x | 1.000 | 0.13x | 1.000 | 0.03x | 0.930 |
| 7.000% | 1.28x | 1.000 | 0.51x | 1.000 | 0.17x | 1.000 | 0.06x | 0.990 | 0.02x | 0.900 |
| 10.000% | 0.76x | 1.000 | 0.29x | 1.000 | 0.11x | 1.000 | 0.03x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 2.10 / 0.08 | 6.09 / 0.09 | 8.68 / 0.11 | 11.13 / 0.15 | 15.60 / 0.60 |
| 0.200% | 1.94 / 0.08 | 3.41 / 0.09 | 4.65 / 0.11 | 6.25 / 0.32 | 7.27 / 1.20 |
| 0.400% | 1.31 / 0.08 | 2.00 / 0.09 | 2.48 / 0.15 | 3.49 / 0.57 | 5.42 / 2.13 |
| 0.700% | 0.99 / 0.08 | 1.20 / 0.11 | 1.80 / 0.24 | 1.90 / 0.94 | 4.59 / 4.58 |
| 1.000% | 0.68 / 0.09 | 0.92 / 0.12 | 1.27 / 0.35 | 1.51 / 1.28 | 3.71 / 6.15 |
| 2.000% | 0.36 / 0.10 | 0.53 / 0.17 | 0.76 / 0.64 | 1.12 / 2.45 | 2.66 / 17.55 |
| 3.500% | 0.34 / 0.11 | 0.55 / 0.31 | 0.47 / 1.03 | 0.99 / 4.58 | 2.13 / 34.93 |
| 5.000% | 0.20 / 0.13 | 0.35 / 0.41 | 0.38 / 1.44 | 0.89 / 6.89 | 1.63 / 54.01 |
| 7.000% | 0.19 / 0.14 | 0.27 / 0.53 | 0.34 / 2.00 | 0.77 / 12.24 | 1.37 / 76.54 |
| 10.000% | 0.15 / 0.20 | 0.21 / 0.72 | 0.31 / 2.80 | 0.67 / 20.28 | 0.99 / 110.18 |

### tag

Query: `@tg:{{{L}}}`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 31.33x | 1.000 | 83.24x | 1.000 | 67.69x | 1.000 | 34.10x | 1.000 | 11.20x | 1.000 |
| 0.200% | 27.40x | 1.000 | 35.45x | 1.000 | 22.70x | 1.000 | 9.40x | 1.000 | 2.71x | 1.000 |
| 0.400% | 15.67x | 1.000 | 15.38x | 1.000 | 8.72x | 1.000 | 2.69x | 0.990 | 1.06x | 1.000 |
| 0.700% | 10.15x | 1.000 | 8.37x | 0.990 | 3.56x | 1.000 | 0.86x | 1.000 | 0.49x | 1.000 |
| 1.000% | 6.80x | 1.000 | 4.64x | 1.000 | 1.61x | 1.000 | 0.52x | 1.000 | 0.29x | 1.000 |
| 2.000% | 2.80x | 1.000 | 1.85x | 1.000 | 0.55x | 0.990 | 0.22x | 1.000 | 0.08x | 0.990 |
| 3.500% | 2.05x | 1.000 | 0.98x | 1.000 | 0.23x | 0.990 | 0.09x | 1.000 | 0.03x | 0.990 |
| 5.000% | 1.11x | 1.000 | 0.51x | 1.000 | 0.14x | 1.000 | 0.06x | 1.000 | 0.02x | 0.930 |
| 7.000% | 0.83x | 1.000 | 0.29x | 1.000 | 0.10x | 1.000 | 0.04x | 0.990 | 0.01x | 0.900 |
| 10.000% | 0.51x | 1.000 | 0.17x | 1.000 | 0.07x | 1.000 | 0.02x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 2.51 / 0.08 | 8.06 / 0.10 | 10.06 / 0.15 | 12.49 / 0.37 | 17.45 / 1.56 |
| 0.200% | 2.34 / 0.09 | 4.03 / 0.11 | 4.79 / 0.21 | 6.73 / 0.72 | 8.09 / 2.99 |
| 0.400% | 1.49 / 0.10 | 2.23 / 0.14 | 2.94 / 0.34 | 3.79 / 1.41 | 6.14 / 5.79 |
| 0.700% | 1.06 / 0.10 | 1.46 / 0.17 | 2.00 / 0.56 | 1.99 / 2.31 | 4.88 / 9.87 |
| 1.000% | 0.76 / 0.11 | 0.96 / 0.21 | 1.25 / 0.77 | 1.67 / 3.19 | 4.21 / 14.37 |
| 2.000% | 0.38 / 0.14 | 0.59 / 0.32 | 0.78 / 1.42 | 1.31 / 5.89 | 2.90 / 37.73 |
| 3.500% | 0.34 / 0.17 | 0.57 / 0.58 | 0.54 / 2.31 | 1.08 / 11.47 | 2.24 / 71.03 |
| 5.000% | 0.22 / 0.20 | 0.38 / 0.76 | 0.42 / 3.08 | 0.94 / 14.90 | 1.73 / 104.21 |
| 7.000% | 0.20 / 0.23 | 0.29 / 1.02 | 0.41 / 4.10 | 0.85 / 23.04 | 1.51 / 142.24 |
| 10.000% | 0.15 / 0.30 | 0.23 / 1.32 | 0.37 / 5.39 | 0.71 / 37.63 | 1.10 / 200.47 |

### text

Query: `@txt:{L}`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 43.91x | 1.000 | 96.23x | 1.000 | 120.52x | 1.000 | 71.30x | 1.000 | 18.67x | 1.000 |
| 0.200% | 36.79x | 1.000 | 56.58x | 1.000 | 42.04x | 1.000 | 16.87x | 1.000 | 4.88x | 1.000 |
| 0.400% | 21.49x | 1.000 | 25.49x | 1.000 | 18.38x | 1.000 | 5.00x | 0.990 | 1.84x | 1.000 |
| 0.700% | 14.61x | 1.000 | 13.37x | 0.990 | 6.21x | 1.000 | 1.50x | 1.000 | 0.67x | 1.000 |
| 1.000% | 9.84x | 1.000 | 7.80x | 1.000 | 2.72x | 1.000 | 0.86x | 1.000 | 0.34x | 1.000 |
| 2.000% | 4.09x | 1.000 | 2.82x | 1.000 | 0.88x | 0.990 | 0.36x | 1.000 | 0.09x | 0.990 |
| 3.500% | 3.13x | 1.000 | 1.41x | 1.000 | 0.35x | 0.990 | 0.14x | 1.000 | 0.04x | 0.990 |
| 5.000% | 1.58x | 1.000 | 0.74x | 1.000 | 0.21x | 1.000 | 0.08x | 1.000 | 0.02x | 0.930 |
| 7.000% | 1.14x | 1.000 | 0.40x | 1.000 | 0.15x | 1.000 | 0.04x | 0.990 | 0.01x | 0.900 |
| 10.000% | 0.62x | 1.000 | 0.21x | 1.000 | 0.08x | 1.000 | 0.02x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 3.47 / 0.08 | 9.20 / 0.10 | 13.85 / 0.11 | 16.40 / 0.23 | 21.33 / 1.14 |
| 0.200% | 3.11 / 0.08 | 5.61 / 0.10 | 5.88 / 0.14 | 8.58 / 0.51 | 10.29 / 2.11 |
| 0.400% | 1.91 / 0.09 | 2.81 / 0.11 | 3.85 / 0.21 | 4.85 / 0.97 | 7.62 / 4.14 |
| 0.700% | 1.35 / 0.09 | 1.76 / 0.13 | 2.43 / 0.39 | 2.51 / 1.67 | 6.05 / 8.98 |
| 1.000% | 0.99 / 0.10 | 1.23 / 0.16 | 1.67 / 0.61 | 2.04 / 2.38 | 4.86 / 14.33 |
| 2.000% | 0.48 / 0.12 | 0.73 / 0.26 | 0.93 / 1.06 | 1.60 / 4.40 | 3.34 / 35.41 |
| 3.500% | 0.44 / 0.14 | 0.67 / 0.47 | 0.63 / 1.77 | 1.25 / 8.65 | 2.55 / 68.75 |
| 5.000% | 0.25 / 0.16 | 0.44 / 0.59 | 0.54 / 2.51 | 1.10 / 13.68 | 2.02 / 101.22 |
| 7.000% | 0.23 / 0.21 | 0.32 / 0.81 | 0.52 / 3.39 | 0.93 / 21.57 | 1.68 / 147.64 |
| 10.000% | 0.17 / 0.28 | 0.23 / 1.10 | 0.39 / 4.87 | 0.81 / 35.07 | 1.30 / 201.71 |

### text OR text

Query: `@txt:{L} | @txt:s5`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 49.66x | 1.000 | 113.31x | 1.000 | 121.31x | 1.000 | 62.60x | 1.000 | 14.02x | 1.000 |
| 0.200% | 42.44x | 1.000 | 56.10x | 1.000 | 43.55x | 1.000 | 16.91x | 1.000 | 4.24x | 1.000 |
| 0.400% | 24.48x | 1.000 | 29.41x | 1.000 | 18.21x | 1.000 | 4.95x | 0.990 | 1.80x | 1.000 |
| 0.700% | 16.22x | 1.000 | 13.37x | 0.990 | 6.47x | 1.000 | 1.53x | 1.000 | 0.71x | 1.000 |
| 1.000% | 11.27x | 1.000 | 8.00x | 1.000 | 2.94x | 1.000 | 0.95x | 1.000 | 0.37x | 1.000 |
| 2.000% | 4.60x | 1.000 | 2.93x | 1.000 | 0.88x | 0.990 | 0.37x | 1.000 | 0.09x | 0.990 |
| 3.500% | 3.54x | 1.000 | 1.58x | 1.000 | 0.36x | 0.990 | 0.15x | 1.000 | 0.04x | 0.990 |
| 5.000% | 1.68x | 1.000 | 0.74x | 1.000 | 0.23x | 1.000 | 0.08x | 1.000 | 0.02x | 0.930 |
| 7.000% | 1.22x | 1.000 | 0.42x | 1.000 | 0.14x | 1.000 | 0.04x | 0.990 | 0.01x | 0.900 |
| 10.000% | 0.64x | 1.000 | 0.23x | 1.000 | 0.09x | 1.000 | 0.02x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 4.08 / 0.08 | 10.87 / 0.10 | 15.29 / 0.13 | 17.93 / 0.29 | 23.00 / 1.64 |
| 0.200% | 3.75 / 0.09 | 6.20 / 0.11 | 6.56 / 0.15 | 9.91 / 0.59 | 11.30 / 2.66 |
| 0.400% | 2.27 / 0.09 | 3.38 / 0.11 | 4.49 / 0.25 | 5.58 / 1.13 | 8.27 / 4.60 |
| 0.700% | 1.57 / 0.10 | 1.99 / 0.15 | 2.75 / 0.42 | 2.71 / 1.77 | 6.78 / 9.57 |
| 1.000% | 1.14 / 0.10 | 1.35 / 0.17 | 1.79 / 0.61 | 2.30 / 2.43 | 5.39 / 14.56 |
| 2.000% | 0.55 / 0.12 | 0.78 / 0.27 | 0.98 / 1.12 | 1.66 / 4.54 | 3.54 / 37.47 |
| 3.500% | 0.51 / 0.14 | 0.72 / 0.46 | 0.65 / 1.83 | 1.42 / 9.32 | 2.59 / 73.68 |
| 5.000% | 0.29 / 0.17 | 0.45 / 0.61 | 0.59 / 2.57 | 1.16 / 14.20 | 2.11 / 108.89 |
| 7.000% | 0.26 / 0.21 | 0.34 / 0.83 | 0.50 / 3.49 | 0.97 / 23.51 | 1.74 / 154.58 |
| 10.000% | 0.20 / 0.31 | 0.26 / 1.15 | 0.43 / 4.99 | 0.85 / 35.42 | 1.29 / 217.01 |

### text AND text

Query: `@txt:{L} @txt:all`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 1.71x | 0.100 | 1.70x | 0.100 | 1.81x | 0.110 | 1.48x | 0.110 | 0.48x | 0.060 |
| 0.200% | 1.37x | 0.100 | 1.50x | 0.100 | 1.29x | 0.110 | 0.66x | 0.110 | 0.23x | 0.060 |
| 0.400% | 1.37x | 0.100 | 1.34x | 0.100 | 0.92x | 0.120 | 0.35x | 0.110 | 0.12x | 0.060 |
| 0.700% | 1.51x | 0.100 | 1.28x | 0.120 | 0.65x | 0.120 | 0.25x | 0.110 | 0.07x | 0.070 |
| 1.000% | 1.23x | 0.100 | 1.03x | 0.120 | 0.46x | 0.130 | 0.17x | 0.110 | 0.05x | 0.070 |
| 2.000% | 1.07x | 0.110 | 0.70x | 0.120 | 0.24x | 0.130 | 0.09x | 0.120 | 0.02x | 0.080 |
| 3.500% | 1.06x | 0.110 | 0.50x | 0.120 | 0.16x | 0.140 | 0.05x | 0.130 | 0.01x | 0.100 |
| 5.000% | 0.83x | 0.110 | 0.31x | 0.150 | 0.11x | 0.170 | 0.03x | 0.130 | 0.01x | 0.120 |
| 7.000% | 0.73x | 0.150 | 0.22x | 0.200 | 0.08x | 0.180 | 0.02x | 0.140 | 0.00x | 0.130 |
| 10.000% | 0.54x | 0.190 | 0.17x | 0.230 | 0.06x | 0.210 | 0.01x | 0.200 | 0.00x | 0.170 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 0.13 / 0.08 | 0.15 / 0.09 | 0.19 / 0.11 | 0.26 / 0.18 | 0.35 / 0.72 |
| 0.200% | 0.12 / 0.09 | 0.14 / 0.09 | 0.17 / 0.13 | 0.23 / 0.35 | 0.31 / 1.33 |
| 0.400% | 0.12 / 0.09 | 0.14 / 0.10 | 0.16 / 0.17 | 0.23 / 0.66 | 0.31 / 2.55 |
| 0.700% | 0.14 / 0.09 | 0.16 / 0.12 | 0.20 / 0.31 | 0.27 / 1.08 | 0.33 / 4.50 |
| 1.000% | 0.12 / 0.10 | 0.15 / 0.15 | 0.20 / 0.44 | 0.25 / 1.50 | 0.32 / 6.73 |
| 2.000% | 0.11 / 0.11 | 0.14 / 0.20 | 0.18 / 0.74 | 0.25 / 2.89 | 0.33 / 18.28 |
| 3.500% | 0.14 / 0.13 | 0.17 / 0.34 | 0.20 / 1.25 | 0.27 / 5.28 | 0.33 / 40.66 |
| 5.000% | 0.13 / 0.16 | 0.15 / 0.47 | 0.20 / 1.71 | 0.29 / 8.81 | 0.33 / 56.08 |
| 7.000% | 0.13 / 0.18 | 0.14 / 0.61 | 0.19 / 2.36 | 0.27 / 14.00 | 0.34 / 82.74 |
| 10.000% | 0.13 / 0.23 | 0.14 / 0.84 | 0.20 / 3.29 | 0.27 / 21.93 | 0.34 / 119.36 |

### num AND tag

Query: `@num:[0 {S}] @tg:{{{L}}}`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 28.45x | 1.000 | 63.14x | 1.000 | 60.52x | 1.000 | 30.94x | 1.000 | 10.00x | 1.000 |
| 0.200% | 21.64x | 1.000 | 32.08x | 1.000 | 19.63x | 1.000 | 7.83x | 1.000 | 2.47x | 1.000 |
| 0.400% | 13.84x | 1.000 | 14.91x | 1.000 | 8.52x | 1.000 | 2.40x | 0.990 | 1.04x | 1.000 |
| 0.700% | 9.62x | 1.000 | 6.93x | 0.990 | 3.10x | 1.000 | 0.79x | 1.000 | 0.41x | 1.000 |
| 1.000% | 6.07x | 1.000 | 4.29x | 1.000 | 1.37x | 1.000 | 0.48x | 1.000 | 0.20x | 1.000 |
| 2.000% | 2.65x | 1.000 | 1.62x | 1.000 | 0.45x | 0.990 | 0.22x | 1.000 | 0.07x | 0.990 |
| 3.500% | 1.97x | 1.000 | 0.87x | 1.000 | 0.22x | 0.990 | 0.10x | 1.000 | 0.03x | 0.990 |
| 5.000% | 1.08x | 1.000 | 0.45x | 1.000 | 0.14x | 1.000 | 0.06x | 1.000 | 0.02x | 0.930 |
| 7.000% | 0.77x | 1.000 | 0.33x | 1.000 | 0.10x | 1.000 | 0.03x | 0.990 | 0.01x | 0.900 |
| 10.000% | 0.51x | 1.000 | 0.21x | 1.000 | 0.07x | 1.000 | 0.02x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 2.26 / 0.08 | 6.28 / 0.10 | 8.90 / 0.15 | 11.52 / 0.37 | 15.82 / 1.58 |
| 0.200% | 2.07 / 0.10 | 3.65 / 0.11 | 4.22 / 0.21 | 6.00 / 0.77 | 7.28 / 2.94 |
| 0.400% | 1.38 / 0.10 | 2.11 / 0.14 | 3.04 / 0.36 | 3.43 / 1.43 | 5.78 / 5.57 |
| 0.700% | 0.99 / 0.10 | 1.32 / 0.19 | 1.79 / 0.58 | 1.80 / 2.27 | 4.49 / 10.90 |
| 1.000% | 0.71 / 0.12 | 0.97 / 0.23 | 1.11 / 0.81 | 1.46 / 3.08 | 3.71 / 18.23 |
| 2.000% | 0.39 / 0.15 | 0.56 / 0.34 | 0.65 / 1.45 | 1.25 / 5.72 | 2.74 / 38.05 |
| 3.500% | 0.37 / 0.19 | 0.55 / 0.63 | 0.51 / 2.34 | 1.02 / 10.19 | 2.19 / 70.60 |
| 5.000% | 0.25 / 0.23 | 0.36 / 0.80 | 0.44 / 3.09 | 0.92 / 15.23 | 1.69 / 101.21 |
| 7.000% | 0.21 / 0.28 | 0.34 / 1.02 | 0.40 / 4.09 | 0.78 / 24.21 | 1.42 / 139.59 |
| 10.000% | 0.18 / 0.35 | 0.27 / 1.30 | 0.35 / 5.22 | 0.69 / 36.37 | 1.03 / 188.95 |

### num AND tag AND text

Query: `@num:[0 {S}] @tg:{{{L}}} @txt:{L}`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 27.31x | 1.000 | 64.22x | 1.000 | 62.23x | 1.000 | 29.87x | 1.000 | 9.51x | 1.000 |
| 0.200% | 22.09x | 1.000 | 29.89x | 1.000 | 19.53x | 1.000 | 8.01x | 1.000 | 2.39x | 1.000 |
| 0.400% | 12.97x | 1.000 | 14.00x | 1.000 | 7.14x | 1.000 | 2.40x | 0.990 | 0.85x | 1.000 |
| 0.700% | 9.58x | 1.000 | 6.32x | 0.990 | 2.96x | 1.000 | 0.81x | 1.000 | 0.41x | 1.000 |
| 1.000% | 6.19x | 1.000 | 4.08x | 1.000 | 1.81x | 1.000 | 0.48x | 1.000 | 0.22x | 1.000 |
| 2.000% | 2.50x | 1.000 | 1.53x | 1.000 | 0.54x | 0.990 | 0.21x | 1.000 | 0.07x | 0.990 |
| 3.500% | 1.77x | 1.000 | 0.81x | 1.000 | 0.21x | 0.990 | 0.09x | 1.000 | 0.03x | 0.990 |
| 5.000% | 0.99x | 1.000 | 0.43x | 1.000 | 0.13x | 1.000 | 0.06x | 1.000 | 0.02x | 0.930 |
| 7.000% | 0.75x | 1.000 | 0.27x | 1.000 | 0.09x | 1.000 | 0.03x | 0.990 | 0.01x | 0.900 |
| 10.000% | 0.49x | 1.000 | 0.17x | 1.000 | 0.06x | 1.000 | 0.02x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 2.20 / 0.08 | 6.47 / 0.10 | 9.50 / 0.15 | 11.66 / 0.39 | 15.83 / 1.66 |
| 0.200% | 2.06 / 0.09 | 3.56 / 0.12 | 4.43 / 0.23 | 6.34 / 0.79 | 7.38 / 3.08 |
| 0.400% | 1.31 / 0.10 | 2.13 / 0.15 | 2.66 / 0.37 | 3.58 / 1.49 | 5.53 / 6.50 |
| 0.700% | 1.04 / 0.11 | 1.26 / 0.20 | 1.89 / 0.64 | 1.90 / 2.34 | 4.60 / 11.35 |
| 1.000% | 0.74 / 0.12 | 0.92 / 0.23 | 1.56 / 0.86 | 1.53 / 3.21 | 3.77 / 16.87 |
| 2.000% | 0.38 / 0.15 | 0.55 / 0.36 | 0.80 / 1.50 | 1.25 / 5.91 | 2.74 / 39.90 |
| 3.500% | 0.33 / 0.18 | 0.53 / 0.65 | 0.52 / 2.42 | 1.03 / 11.06 | 2.09 / 70.47 |
| 5.000% | 0.22 / 0.22 | 0.36 / 0.82 | 0.43 / 3.29 | 0.93 / 15.47 | 1.72 / 100.06 |
| 7.000% | 0.20 / 0.27 | 0.28 / 1.06 | 0.40 / 4.25 | 0.79 / 26.65 | 1.39 / 139.33 |
| 10.000% | 0.18 / 0.36 | 0.22 / 1.34 | 0.36 / 5.60 | 0.68 / 37.14 | 1.00 / 193.36 |

### num AND NOT tag

Query: `@num:[0 {S}] -@tg:{{none}}`

gain = inline_p50 / prefilter_p50 (>1 favours pre-filter); r = inline recall

| selectivity | N=5,000 gain | r | N=20,000 gain | r | N=80,000 gain | r | N=320,000 gain | r | N=1,280,000 gain | r |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.100% | 28.25x | 1.000 | 66.05x | 1.000 | 56.36x | 1.000 | 27.14x | 1.000 | 9.70x | 1.000 |
| 0.200% | 22.89x | 1.000 | 29.49x | 1.000 | 20.29x | 1.000 | 7.56x | 1.000 | 2.37x | 1.000 |
| 0.400% | 13.97x | 1.000 | 14.55x | 1.000 | 8.04x | 1.000 | 2.24x | 0.990 | 0.94x | 1.000 |
| 0.700% | 9.11x | 1.000 | 7.14x | 0.990 | 2.98x | 1.000 | 0.85x | 1.000 | 0.41x | 1.000 |
| 1.000% | 6.13x | 1.000 | 4.48x | 1.000 | 1.45x | 1.000 | 0.48x | 1.000 | 0.25x | 1.000 |
| 2.000% | 2.63x | 1.000 | 1.54x | 1.000 | 0.49x | 0.990 | 0.21x | 1.000 | 0.07x | 0.990 |
| 3.500% | 1.86x | 1.000 | 0.88x | 1.000 | 0.22x | 0.990 | 0.09x | 1.000 | 0.03x | 0.990 |
| 5.000% | 1.13x | 1.000 | 0.44x | 1.000 | 0.14x | 1.000 | 0.06x | 1.000 | 0.02x | 0.930 |
| 7.000% | 0.75x | 1.000 | 0.27x | 1.000 | 0.09x | 1.000 | 0.03x | 0.990 | 0.01x | 0.900 |
| 10.000% | 0.48x | 1.000 | 0.16x | 1.000 | 0.07x | 1.000 | 0.02x | 0.980 | 0.01x | 0.770 |

Raw serial latency (ms, p50): inline / prefilter

| selectivity | N=5,000 | N=20,000 | N=80,000 | N=320,000 | N=1,280,000 |
|---|---|---|---|---|---|
| 0.100% | 2.23 / 0.08 | 6.67 / 0.10 | 8.70 / 0.15 | 11.28 / 0.42 | 15.75 / 1.62 |
| 0.200% | 2.08 / 0.09 | 3.58 / 0.12 | 4.36 / 0.21 | 6.35 / 0.84 | 7.29 / 3.07 |
| 0.400% | 1.35 / 0.10 | 2.20 / 0.15 | 2.81 / 0.35 | 3.56 / 1.59 | 5.45 / 5.78 |
| 0.700% | 0.99 / 0.11 | 1.30 / 0.18 | 1.80 / 0.60 | 1.98 / 2.33 | 4.45 / 10.98 |
| 1.000% | 0.74 / 0.12 | 0.98 / 0.22 | 1.22 / 0.84 | 1.51 / 3.15 | 3.71 / 14.74 |
| 2.000% | 0.40 / 0.15 | 0.57 / 0.37 | 0.70 / 1.44 | 1.21 / 5.88 | 2.82 / 37.68 |
| 3.500% | 0.35 / 0.19 | 0.53 / 0.61 | 0.52 / 2.37 | 1.02 / 11.07 | 2.10 / 70.98 |
| 5.000% | 0.24 / 0.21 | 0.34 / 0.77 | 0.42 / 3.10 | 0.92 / 15.67 | 1.71 / 103.42 |
| 7.000% | 0.19 / 0.25 | 0.27 / 1.00 | 0.38 / 4.11 | 0.79 / 23.25 | 1.41 / 142.80 |
| 10.000% | 0.15 / 0.32 | 0.20 / 1.26 | 0.35 / 5.41 | 0.67 / 36.08 | 1.08 / 191.35 |

## Saturating load: does the choice matter at this N?

If both paths pin to the same request-rate ceiling then the threshold barely matters at that corpus size. `MAIN-BOUND` means valkey's main thread is the limit (near 100% of one core), `reader-bound` means the search threads are, `client-bound` means the load generator was.

| pattern | N | selectivity | inline qps | prefilter qps | qps gain | in cpu | pre cpu | in main | pre main | bound |
|---|---|---|---|---|---|---|---|---|---|---|
| num AND NOT tag | 5,000 | 0.500% | 3572 | 12961 | 3.63x | 414% | 96% | 12% | 40% | reader-bound |
| num AND NOT tag | 5,000 | 2.000% | 11795 | 12830 | 1.09x | 439% | 156% | 38% | 40% | reader-bound |
| num AND NOT tag | 5,000 | 10.000% | 12260 | 12034 | 0.98x | 171% | 380% | 40% | 40% | reader-bound |
| num AND NOT tag | 20,000 | 0.500% | 2718 | 12768 | 4.70x | 411% | 187% | 10% | 41% | reader-bound |
| num AND NOT tag | 20,000 | 2.000% | 8981 | 10690 | 1.19x | 437% | 439% | 28% | 33% | reader-bound |
| num AND NOT tag | 20,000 | 10.000% | 11955 | 3032 | 0.25x | 208% | 412% | 39% | 11% | reader-bound |
| num AND NOT tag | 80,000 | 0.500% | 2418 | 8428 | 3.49x | 410% | 431% | 9% | 27% | reader-bound |
| num AND NOT tag | 80,000 | 2.000% | 9044 | 2492 | 0.28x | 435% | 410% | 30% | 8% | reader-bound |
| num AND NOT tag | 80,000 | 10.000% | 11713 | 719 | 0.06x | 286% | 403% | 39% | 3% | reader-bound |
| num AND NOT tag | 320,000 | 0.500% | 1904 | 2034 | 1.07x | 407% | 409% | 7% | 7% | reader-bound |
| num AND NOT tag | 320,000 | 2.000% | 5193 | 636 | 0.12x | 420% | 403% | 17% | 2% | reader-bound |
| num AND NOT tag | 320,000 | 10.000% | 10746 | 95 | 0.01x | 438% | 400% | 35% | 1% | reader-bound |
| num AND NOT tag | 1,280,000 | 0.500% | 856 | 513 | 0.60x | 404% | 402% | 4% | 2% | reader-bound |
| num AND NOT tag | 1,280,000 | 2.000% | 1768 | 97 | 0.06x | 408% | 400% | 7% | 1% | reader-bound |
| num AND NOT tag | 1,280,000 | 10.000% | 5785 | 20 | 0.00x | 432% | 400% | 28% | 1% | reader-bound |
| num AND tag | 5,000 | 0.500% | 3523 | 12947 | 3.67x | 416% | 94% | 11% | 41% | reader-bound |
| num AND tag | 5,000 | 2.000% | 11733 | 12852 | 1.10x | 439% | 152% | 41% | 41% | reader-bound |
| num AND tag | 5,000 | 10.000% | 12434 | 12130 | 0.98x | 171% | 379% | 40% | 38% | reader-bound |
| num AND tag | 20,000 | 0.500% | 2805 | 12790 | 4.56x | 411% | 182% | 10% | 40% | reader-bound |
| num AND tag | 20,000 | 2.000% | 9177 | 11297 | 1.23x | 434% | 440% | 29% | 35% | reader-bound |
| num AND tag | 20,000 | 10.000% | 12225 | 3018 | 0.25x | 212% | 412% | 39% | 11% | reader-bound |
| num AND tag | 80,000 | 0.500% | 2366 | 8802 | 3.72x | 410% | 432% | 9% | 27% | reader-bound |
| num AND tag | 80,000 | 2.000% | 8841 | 2523 | 0.29x | 433% | 410% | 28% | 9% | reader-bound |
| num AND tag | 80,000 | 10.000% | 11861 | 720 | 0.06x | 284% | 402% | 38% | 3% | reader-bound |
| num AND tag | 320,000 | 0.500% | 1850 | 2075 | 1.12x | 407% | 409% | 7% | 7% | reader-bound |
| num AND tag | 320,000 | 2.000% | 5075 | 633 | 0.12x | 418% | 403% | 17% | 3% | reader-bound |
| num AND tag | 320,000 | 10.000% | 9710 | 96 | 0.01x | 432% | 401% | 30% | 1% | reader-bound |
| num AND tag | 1,280,000 | 0.500% | 867 | 528 | 0.61x | 404% | 402% | 4% | 2% | reader-bound |
| num AND tag | 1,280,000 | 2.000% | 1737 | 97 | 0.06x | 408% | 401% | 8% | 1% | reader-bound |
| num AND tag | 1,280,000 | 10.000% | 5957 | 20 | 0.00x | 423% | 400% | 20% | 1% | reader-bound |
| num AND tag AND text | 5,000 | 0.500% | 3655 | 12853 | 3.52x | 414% | 99% | 13% | 42% | reader-bound |
| num AND tag AND text | 5,000 | 2.000% | 11735 | 12772 | 1.09x | 435% | 162% | 38% | 43% | reader-bound |
| num AND tag AND text | 5,000 | 10.000% | 12335 | 12037 | 0.98x | 171% | 395% | 41% | 40% | reader-bound |
| num AND tag AND text | 20,000 | 0.500% | 2698 | 12762 | 4.73x | 412% | 192% | 10% | 42% | reader-bound |
| num AND tag AND text | 20,000 | 2.000% | 9110 | 10328 | 1.13x | 436% | 442% | 31% | 36% | reader-bound |
| num AND tag AND text | 20,000 | 10.000% | 12041 | 2837 | 0.24x | 211% | 412% | 40% | 10% | reader-bound |
| num AND tag AND text | 80,000 | 0.500% | 2380 | 8515 | 3.58x | 412% | 433% | 9% | 28% | reader-bound |
| num AND tag AND text | 80,000 | 2.000% | 8836 | 2405 | 0.27x | 434% | 410% | 30% | 8% | reader-bound |
| num AND tag AND text | 80,000 | 10.000% | 11742 | 679 | 0.06x | 289% | 403% | 40% | 3% | reader-bound |
| num AND tag AND text | 320,000 | 0.500% | 1905 | 2031 | 1.07x | 408% | 413% | 7% | 12% | reader-bound |
| num AND tag AND text | 320,000 | 2.000% | 5335 | 617 | 0.12x | 420% | 403% | 18% | 3% | reader-bound |
| num AND tag AND text | 320,000 | 10.000% | 11046 | 89 | 0.01x | 443% | 401% | 39% | 1% | reader-bound |
| num AND tag AND text | 1,280,000 | 0.500% | 857 | 523 | 0.61x | 404% | 402% | 4% | 3% | reader-bound |
| num AND tag AND text | 1,280,000 | 2.000% | 1627 | 92 | 0.06x | 408% | 401% | 7% | 2% | reader-bound |
| num AND tag AND text | 1,280,000 | 10.000% | 5708 | 19 | 0.00x | 422% | 400% | 20% | 0% | reader-bound |
| numeric | 5,000 | 0.500% | 3863 | 12895 | 3.34x | 414% | 70% | 12% | 40% | reader-bound |
| numeric | 5,000 | 2.000% | 11840 | 13063 | 1.10x | 415% | 88% | 37% | 40% | reader-bound |
| numeric | 5,000 | 10.000% | 12039 | 12624 | 1.05x | 148% | 208% | 36% | 38% | client-bound |
| numeric | 20,000 | 0.500% | 2869 | 13011 | 4.54x | 411% | 89% | 10% | 40% | reader-bound |
| numeric | 20,000 | 2.000% | 9476 | 12459 | 1.31x | 433% | 172% | 29% | 39% | reader-bound |
| numeric | 20,000 | 10.000% | 12250 | 6591 | 0.54x | 190% | 430% | 38% | 24% | reader-bound |
| numeric | 80,000 | 0.500% | 2522 | 12608 | 5.00x | 409% | 189% | 8% | 39% | reader-bound |
| numeric | 80,000 | 2.000% | 9506 | 7258 | 0.76x | 435% | 430% | 31% | 24% | reader-bound |
| numeric | 80,000 | 10.000% | 11845 | 1434 | 0.12x | 265% | 406% | 40% | 5% | reader-bound |
| numeric | 320,000 | 0.500% | 1882 | 6746 | 3.59x | 408% | 424% | 7% | 21% | reader-bound |
| numeric | 320,000 | 2.000% | 5206 | 1724 | 0.33x | 418% | 407% | 17% | 6% | reader-bound |
| numeric | 320,000 | 10.000% | 11501 | 168 | 0.01x | 436% | 401% | 35% | 1% | reader-bound |
| numeric | 1,280,000 | 0.500% | 859 | 1567 | 1.82x | 404% | 406% | 4% | 6% | reader-bound |
| numeric | 1,280,000 | 2.000% | 1861 | 215 | 0.12x | 408% | 401% | 7% | 1% | reader-bound |
| numeric | 1,280,000 | 10.000% | 5911 | 35 | 0.01x | 420% | 400% | 19% | 1% | reader-bound |
| tag | 5,000 | 0.500% | 3203 | 13032 | 4.07x | 413% | 90% | 10% | 40% | reader-bound |
| tag | 5,000 | 2.000% | 11330 | 12956 | 1.14x | 439% | 144% | 34% | 39% | reader-bound |
| tag | 5,000 | 10.000% | 12263 | 12089 | 0.99x | 166% | 357% | 38% | 37% | reader-bound |
| tag | 20,000 | 0.500% | 2501 | 12840 | 5.13x | 412% | 173% | 9% | 39% | reader-bound |
| tag | 20,000 | 2.000% | 8256 | 11010 | 1.33x | 429% | 440% | 26% | 34% | reader-bound |
| tag | 20,000 | 10.000% | 11912 | 2961 | 0.25x | 204% | 411% | 38% | 10% | reader-bound |
| tag | 80,000 | 0.500% | 2161 | 8876 | 4.11x | 409% | 433% | 8% | 28% | reader-bound |
| tag | 80,000 | 2.000% | 8257 | 2498 | 0.30x | 428% | 409% | 25% | 8% | reader-bound |
| tag | 80,000 | 10.000% | 12048 | 695 | 0.06x | 292% | 403% | 38% | 3% | reader-bound |
| tag | 320,000 | 0.500% | 1689 | 2174 | 1.29x | 406% | 409% | 7% | 7% | reader-bound |
| tag | 320,000 | 2.000% | 5027 | 630 | 0.13x | 417% | 403% | 16% | 3% | reader-bound |
| tag | 320,000 | 10.000% | 9966 | 89 | 0.01x | 432% | 400% | 30% | 1% | reader-bound |
| tag | 1,280,000 | 0.500% | 795 | 524 | 0.66x | 403% | 404% | 4% | 4% | reader-bound |
| tag | 1,280,000 | 2.000% | 1757 | 97 | 0.05x | 409% | 400% | 8% | 1% | reader-bound |
| tag | 1,280,000 | 10.000% | 5792 | 18 | 0.00x | 422% | 400% | 20% | 1% | reader-bound |
| text | 5,000 | 0.500% | 2085 | 13064 | 6.27x | 408% | 77% | 7% | 41% | reader-bound |
| text | 5,000 | 2.000% | 7855 | 13046 | 1.66x | 428% | 113% | 24% | 40% | reader-bound |
| text | 5,000 | 10.000% | 12368 | 12152 | 0.98x | 202% | 357% | 40% | 39% | reader-bound |
| text | 20,000 | 0.500% | 1849 | 12911 | 6.98x | 407% | 111% | 7% | 40% | reader-bound |
| text | 20,000 | 2.000% | 6391 | 12052 | 1.89x | 423% | 313% | 21% | 39% | reader-bound |
| text | 20,000 | 10.000% | 11931 | 3140 | 0.26x | 242% | 413% | 37% | 12% | reader-bound |
| text | 80,000 | 0.500% | 1656 | 12247 | 7.40x | 407% | 348% | 6% | 40% | reader-bound |
| text | 80,000 | 2.000% | 6188 | 3598 | 0.58x | 424% | 413% | 21% | 12% | reader-bound |
| text | 80,000 | 10.000% | 11972 | 719 | 0.06x | 355% | 403% | 42% | 3% | reader-bound |
| text | 320,000 | 0.500% | 1237 | 3184 | 2.57x | 405% | 412% | 5% | 11% | reader-bound |
| text | 320,000 | 2.000% | 3595 | 804 | 0.22x | 413% | 405% | 12% | 6% | reader-bound |
| text | 320,000 | 10.000% | 8515 | 99 | 0.01x | 428% | 401% | 28% | 2% | reader-bound |
| text | 1,280,000 | 0.500% | 623 | 702 | 1.13x | 403% | 403% | 3% | 4% | reader-bound |
| text | 1,280,000 | 2.000% | 1362 | 101 | 0.07x | 406% | 401% | 5% | 1% | reader-bound |
| text | 1,280,000 | 10.000% | 5077 | 20 | 0.00x | 420% | 400% | 17% | 1% | reader-bound |
| text AND text | 5,000 | 0.500% | 12514 | 13028 | 1.04x | 105% | 80% | 41% | 42% | client-bound |
| text AND text | 5,000 | 2.000% | 12407 | 12901 | 1.04x | 104% | 106% | 40% | 42% | client-bound |
| text AND text | 5,000 | 10.000% | 12419 | 12167 | 0.98x | 104% | 266% | 41% | 42% | client-bound |
| text AND text | 20,000 | 0.500% | 12212 | 12885 | 1.06x | 116% | 107% | 40% | 41% | client-bound |
| text AND text | 20,000 | 2.000% | 12483 | 12328 | 0.99x | 118% | 219% | 41% | 41% | client-bound |
| text AND text | 20,000 | 10.000% | 12325 | 5526 | 0.45x | 117% | 421% | 40% | 19% | reader-bound |
| text AND text | 80,000 | 0.500% | 12324 | 12305 | 1.00x | 149% | 233% | 42% | 41% | client-bound |
| text AND text | 80,000 | 2.000% | 12212 | 6090 | 0.50x | 146% | 424% | 41% | 22% | reader-bound |
| text AND text | 80,000 | 10.000% | 12195 | 1257 | 0.10x | 148% | 405% | 41% | 5% | reader-bound |
| text AND text | 320,000 | 0.500% | 12183 | 5547 | 0.46x | 196% | 422% | 41% | 19% | reader-bound |
| text AND text | 320,000 | 2.000% | 11967 | 1460 | 0.12x | 191% | 406% | 40% | 6% | reader-bound |
| text AND text | 320,000 | 10.000% | 12303 | 146 | 0.01x | 198% | 401% | 42% | 1% | reader-bound |
| text AND text | 1,280,000 | 0.500% | 11748 | 1331 | 0.11x | 239% | 406% | 41% | 5% | reader-bound |
| text AND text | 1,280,000 | 2.000% | 12000 | 173 | 0.01x | 244% | 401% | 42% | 1% | reader-bound |
| text AND text | 1,280,000 | 10.000% | 11964 | 32 | 0.00x | 246% | 400% | 41% | 1% | reader-bound |
| text OR text | 5,000 | 0.500% | 1483 | 13023 | 8.78x | 405% | 84% | 5% | 43% | reader-bound |
| text OR text | 5,000 | 2.000% | 6252 | 13073 | 2.09x | 424% | 120% | 21% | 42% | reader-bound |
| text OR text | 5,000 | 10.000% | 11950 | 12361 | 1.03x | 221% | 376% | 39% | 44% | reader-bound |
| text OR text | 20,000 | 0.500% | 1472 | 12885 | 8.75x | 406% | 123% | 6% | 41% | reader-bound |
| text OR text | 20,000 | 2.000% | 5408 | 12130 | 2.24x | 421% | 339% | 18% | 41% | reader-bound |
| text OR text | 20,000 | 10.000% | 11816 | 3018 | 0.26x | 275% | 412% | 39% | 11% | reader-bound |
| text OR text | 80,000 | 0.500% | 1395 | 11947 | 8.57x | 406% | 383% | 6% | 40% | reader-bound |
| text OR text | 80,000 | 2.000% | 5117 | 3522 | 0.69x | 420% | 413% | 17% | 12% | reader-bound |
| text OR text | 80,000 | 10.000% | 11978 | 688 | 0.06x | 395% | 403% | 41% | 3% | reader-bound |
| text OR text | 320,000 | 0.500% | 1062 | 2962 | 2.79x | 405% | 412% | 5% | 10% | reader-bound |
| text OR text | 320,000 | 2.000% | 3189 | 783 | 0.25x | 413% | 403% | 12% | 4% | reader-bound |
| text OR text | 320,000 | 10.000% | 8030 | 96 | 0.01x | 431% | 401% | 26% | 1% | reader-bound |
| text OR text | 1,280,000 | 0.500% | 565 | 615 | 1.09x | 402% | 402% | 3% | 3% | reader-bound |
| text OR text | 1,280,000 | 2.000% | 1323 | 96 | 0.07x | 407% | 400% | 6% | 1% | reader-bound |
| text OR text | 1,280,000 | 10.000% | 4577 | 18 | 0.00x | 417% | 400% | 16% | 0% | reader-bound |

