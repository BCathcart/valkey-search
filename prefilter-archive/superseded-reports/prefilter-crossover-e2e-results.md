# End-to-end pre-filter vs inline-filter crossover (saturating load)

- N = 20000, dim = 768, k = 10
- 16 concurrent clients, 4 reader threads, 4 writer threads, 32 cores
- 3.0s of load per point, 1.0s warmup
- Full stack: real valkey-server + module, RESP clients, real planner / entries fetchers / dedup / reply gen
- Paths forced via `search.prefiltering-threshold-ratio` = 0.0 (inline) vs 1.0 (pre-filter)
- `gain` = prefilter_qps / inline_qps; above 1.0 favours pre-filtering
- corpus distribution: clustered
- `in_recall` = mean recall of the approximate inline path against the exact pre-filter result, over 20 fixed query vectors run serially. Pre-filter is a brute-force exact scan of the qualifying set, so it is ground truth. Values below 1.0 mean inline is returning worse neighbours, so its higher throughput is partly bought with answer quality.
- `cpu` = server process CPU over the window. Saturation target is ~400% (4 reader threads fully busy). Values near that target mean the measurement is throughput-bound rather than client-bound.

| query shape | selectivity | qualified | inline qps | prefilter qps | gain | in p50 | pre p50 | in p90 | pre p90 | in p99 | pre p99 | in cpu | pre cpu | bound | winner | in_recall |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| numeric | 0.50% | 100 | 2835.4 | 12948.1 | 4.57x | 5.60 | 1.09 | 6.37 | 2.15 | 7.05 | 3.52 | 411% | 89% | reader-bound | prefilter | 1.000 |
| numeric | 1.00% | 200 | 4832.5 | 12914.2 | 2.67x | 3.27 | 1.09 | 3.80 | 2.16 | 4.34 | 3.50 | 418% | 112% | reader-bound | prefilter | 1.000 |
| numeric | 2.00% | 400 | 9806.5 | 12594.9 | 1.28x | 1.61 | 1.11 | 1.96 | 2.23 | 2.20 | 3.71 | 435% | 177% | reader-bound | prefilter | 1.000 |
| numeric | 5.00% | 1000 | 12060.5 | 12108.0 | 1.00x | 1.16 | 1.19 | 2.23 | 2.06 | 3.85 | 3.35 | 317% | 421% | reader-bound | prefilter | 1.000 |
| numeric | 10.00% | 2000 | 12163.2 | 6381.9 | 0.52x | 1.16 | 2.49 | 2.29 | 2.58 | 3.79 | 2.74 | 191% | 423% | reader-bound | inline | 0.995 |
| numeric | 20.00% | 4000 | 12343.5 | 3377.4 | 0.27x | 1.14 | 4.72 | 2.26 | 4.95 | 3.78 | 5.08 | 147% | 413% | reader-bound | inline | 1.000 |
| numeric | 30.00% | 6000 | 12447.7 | 2287.8 | 0.18x | 1.12 | 6.97 | 2.25 | 7.15 | 3.76 | 7.39 | 134% | 409% | reader-bound | inline | 1.000 |
| numeric | 40.00% | 8000 | 12477.3 | 1722.8 | 0.14x | 1.13 | 9.18 | 2.22 | 9.68 | 3.65 | 9.89 | 128% | 407% | reader-bound | inline | 1.000 |
| tag | 0.50% | 100 | 2357.8 | 12757.8 | 5.41x | 6.74 | 1.11 | 7.66 | 2.17 | 8.50 | 3.52 | 409% | 160% | reader-bound | prefilter | 1.000 |
| tag | 1.00% | 200 | 4154.4 | 12548.1 | 3.02x | 3.80 | 1.13 | 4.45 | 2.13 | 5.18 | 3.39 | 416% | 256% | reader-bound | prefilter | 1.000 |
| tag | 2.00% | 400 | 7790.1 | 11128.5 | 1.43x | 2.01 | 1.40 | 2.47 | 1.70 | 2.80 | 2.83 | 431% | 434% | reader-bound | prefilter | 1.000 |
| tag | 5.00% | 1000 | 11721.1 | 4613.1 | 0.39x | 1.20 | 3.46 | 2.27 | 3.54 | 3.83 | 3.68 | 352% | 417% | reader-bound | inline | 1.000 |
| tag | 10.00% | 2000 | 11977.7 | 2361.3 | 0.20x | 1.17 | 6.77 | 2.31 | 6.89 | 3.87 | 7.09 | 214% | 409% | reader-bound | inline | 0.995 |
| tag | 20.00% | 4000 | 12298.0 | 1337.3 | 0.11x | 1.14 | 11.91 | 2.27 | 12.18 | 3.73 | 12.66 | 164% | 405% | reader-bound | inline | 1.000 |
| tag | 30.00% | 6000 | 12150.7 | 955.4 | 0.08x | 1.15 | 16.73 | 2.31 | 17.02 | 3.87 | 17.28 | 147% | 404% | reader-bound | inline | 1.000 |
| tag | 40.00% | 8000 | 12208.7 | 775.3 | 0.06x | 1.15 | 20.54 | 2.31 | 21.01 | 3.75 | 22.05 | 138% | 403% | reader-bound | inline | 1.000 |
| text | 0.50% | 100 | 1858.8 | 12845.0 | 6.91x | 8.55 | 1.10 | 9.75 | 2.17 | 10.73 | 3.48 | 408% | 110% | reader-bound | prefilter | 1.000 |
| text | 1.00% | 200 | 3394.3 | 12601.6 | 3.71x | 4.65 | 1.12 | 5.44 | 2.20 | 6.29 | 3.56 | 413% | 163% | reader-bound | prefilter | 1.000 |
| text | 2.00% | 400 | 6461.2 | 12149.3 | 1.88x | 2.42 | 1.18 | 2.99 | 2.18 | 3.43 | 3.49 | 427% | 298% | reader-bound | prefilter | 1.000 |
| text | 5.00% | 1000 | 11852.9 | 6203.9 | 0.52x | 1.23 | 2.55 | 2.09 | 2.65 | 3.35 | 2.83 | 413% | 421% | reader-bound | inline | 1.000 |
| text | 10.00% | 2000 | 12236.7 | 3435.3 | 0.28x | 1.16 | 4.62 | 2.23 | 4.81 | 3.63 | 4.96 | 251% | 413% | reader-bound | inline | 0.995 |
| text | 20.00% | 4000 | 12096.9 | 1753.1 | 0.14x | 1.16 | 9.10 | 2.31 | 9.38 | 3.84 | 9.61 | 185% | 407% | reader-bound | inline | 1.000 |
| text | 30.00% | 6000 | 12231.5 | 1033.0 | 0.08x | 1.15 | 15.43 | 2.28 | 15.81 | 3.76 | 16.09 | 167% | 404% | reader-bound | inline | 1.000 |
| text | 40.00% | 8000 | 12356.6 | 808.8 | 0.07x | 1.13 | 19.77 | 2.27 | 20.10 | 3.78 | 20.34 | 156% | 404% | reader-bound | inline | 1.000 |
| num AND tag | 0.50% | 100 | 2778.5 | 12739.0 | 4.58x | 5.71 | 1.11 | 6.50 | 2.19 | 7.20 | 3.53 | 412% | 171% | reader-bound | prefilter | 1.000 |
| num AND tag | 1.00% | 200 | 4745.9 | 12526.0 | 2.64x | 3.33 | 1.14 | 3.85 | 2.13 | 4.42 | 3.42 | 420% | 266% | reader-bound | prefilter | 1.000 |
| num AND tag | 2.00% | 400 | 9243.3 | 11254.3 | 1.22x | 1.71 | 1.41 | 2.05 | 1.57 | 2.30 | 2.13 | 434% | 439% | reader-bound | prefilter | 1.000 |
| num AND tag | 5.00% | 1000 | 11777.0 | 4592.1 | 0.39x | 1.18 | 3.46 | 2.29 | 3.58 | 3.94 | 3.86 | 328% | 417% | reader-bound | inline | 1.000 |
| num AND tag | 10.00% | 2000 | 11971.9 | 2278.1 | 0.19x | 1.17 | 7.00 | 2.34 | 7.12 | 3.87 | 7.71 | 207% | 409% | reader-bound | inline | 0.995 |
| num AND tag | 20.00% | 4000 | 12374.7 | 1351.2 | 0.11x | 1.13 | 11.80 | 2.27 | 12.10 | 3.74 | 13.00 | 167% | 406% | reader-bound | inline | 1.000 |
| num AND tag | 30.00% | 6000 | 12300.7 | 988.4 | 0.08x | 1.14 | 16.14 | 2.28 | 16.61 | 3.77 | 16.97 | 152% | 405% | reader-bound | inline | 1.000 |
| num AND tag | 40.00% | 8000 | 12295.0 | 815.2 | 0.07x | 1.14 | 19.52 | 2.30 | 20.29 | 3.75 | 20.69 | 144% | 404% | reader-bound | inline | 1.000 |
| num AND text | 0.50% | 100 | 2715.9 | 12755.4 | 4.70x | 5.85 | 1.11 | 6.67 | 2.18 | 7.50 | 3.59 | 414% | 102% | reader-bound | prefilter | 1.000 |
| num AND text | 1.00% | 200 | 4747.2 | 12776.2 | 2.69x | 3.32 | 1.10 | 3.88 | 2.19 | 4.43 | 3.58 | 421% | 134% | reader-bound | prefilter | 1.000 |
| num AND text | 2.00% | 400 | 9210.5 | 12427.7 | 1.35x | 1.71 | 1.13 | 2.07 | 2.23 | 2.33 | 3.65 | 434% | 216% | reader-bound | prefilter | 1.000 |
| num AND text | 5.00% | 1000 | 11903.2 | 10729.5 | 0.90x | 1.18 | 1.48 | 2.24 | 1.58 | 3.86 | 1.84 | 327% | 439% | reader-bound | inline | 1.000 |
| num AND text | 10.00% | 2000 | 12076.2 | 5301.9 | 0.44x | 1.15 | 3.00 | 2.32 | 3.14 | 3.92 | 3.31 | 202% | 420% | reader-bound | inline | 0.995 |
| num AND text | 20.00% | 4000 | 12100.2 | 2646.4 | 0.22x | 1.16 | 6.04 | 2.33 | 6.26 | 3.85 | 6.48 | 152% | 416% | reader-bound | inline | 1.000 |
| num AND text | 30.00% | 6000 | 12315.9 | 1823.9 | 0.15x | 1.13 | 8.75 | 2.29 | 8.98 | 3.79 | 9.57 | 141% | 408% | reader-bound | inline | 1.000 |
| num AND text | 40.00% | 8000 | 12415.1 | 1377.3 | 0.11x | 1.13 | 11.64 | 2.26 | 12.04 | 3.78 | 12.30 | 135% | 406% | reader-bound | inline | 1.000 |
| tag AND text | 0.50% | 100 | 2283.7 | 12755.4 | 5.59x | 6.96 | 1.11 | 7.88 | 2.17 | 8.72 | 3.53 | 409% | 171% | reader-bound | prefilter | 1.000 |
| tag AND text | 1.00% | 200 | 4163.6 | 12404.9 | 2.98x | 3.78 | 1.14 | 4.44 | 2.14 | 5.09 | 3.38 | 417% | 271% | reader-bound | prefilter | 1.000 |
| tag AND text | 2.00% | 400 | 7878.5 | 11369.8 | 1.44x | 1.99 | 1.39 | 2.45 | 1.52 | 2.77 | 1.84 | 430% | 440% | reader-bound | prefilter | 1.000 |
| tag AND text | 5.00% | 1000 | 11872.9 | 4496.8 | 0.38x | 1.20 | 3.54 | 2.22 | 3.63 | 3.69 | 3.91 | 362% | 418% | reader-bound | inline | 1.000 |
| tag AND text | 10.00% | 2000 | 11583.9 | 2325.1 | 0.20x | 1.21 | 6.85 | 2.39 | 6.99 | 3.98 | 7.39 | 221% | 410% | reader-bound | inline | 0.995 |
| tag AND text | 20.00% | 4000 | 12159.8 | 1383.5 | 0.11x | 1.15 | 11.53 | 2.31 | 11.74 | 3.85 | 12.35 | 170% | 406% | reader-bound | inline | 1.000 |
| tag AND text | 30.00% | 6000 | 12176.6 | 989.0 | 0.08x | 1.15 | 16.08 | 2.31 | 16.50 | 3.80 | 16.92 | 155% | 404% | reader-bound | inline | 1.000 |
| tag AND text | 40.00% | 8000 | 12424.8 | 825.5 | 0.07x | 1.12 | 19.26 | 2.27 | 19.76 | 3.73 | 20.25 | 148% | 404% | reader-bound | inline | 1.000 |
| num AND tag AND text | 0.50% | 100 | 2793.8 | 12865.5 | 4.60x | 5.67 | 1.10 | 6.51 | 2.16 | 7.19 | 3.49 | 412% | 179% | reader-bound | prefilter | 1.000 |
| num AND tag AND text | 1.00% | 200 | 4737.0 | 12416.8 | 2.62x | 3.33 | 1.15 | 3.88 | 2.15 | 4.50 | 3.37 | 419% | 278% | reader-bound | prefilter | 1.000 |
| num AND tag AND text | 2.00% | 400 | 8924.5 | 10857.3 | 1.22x | 1.76 | 1.46 | 2.16 | 1.56 | 2.49 | 1.73 | 435% | 440% | reader-bound | prefilter | 1.000 |
| num AND tag AND text | 5.00% | 1000 | 11976.4 | 4377.9 | 0.37x | 1.17 | 3.64 | 2.23 | 3.74 | 3.73 | 4.07 | 339% | 418% | reader-bound | inline | 1.000 |
| num AND tag AND text | 10.00% | 2000 | 12214.4 | 2301.7 | 0.19x | 1.15 | 6.91 | 2.25 | 7.08 | 3.72 | 7.69 | 216% | 410% | reader-bound | inline | 0.995 |
| num AND tag AND text | 20.00% | 4000 | 12095.6 | 1264.2 | 0.10x | 1.16 | 12.63 | 2.34 | 12.88 | 3.81 | 13.78 | 166% | 406% | reader-bound | inline | 1.000 |
| num AND tag AND text | 30.00% | 6000 | 12390.8 | 916.7 | 0.07x | 1.14 | 17.35 | 2.26 | 17.88 | 3.71 | 18.47 | 155% | 404% | reader-bound | inline | 1.000 |
| num AND tag AND text | 40.00% | 8000 | 12334.4 | 770.7 | 0.06x | 1.14 | 20.72 | 2.28 | 21.12 | 3.71 | 21.87 | 146% | 404% | reader-bound | inline | 1.000 |
| num OR tag | 0.50% | 100 | 2329.4 | 12917.3 | 5.55x | 6.82 | 1.09 | 7.80 | 2.16 | 8.71 | 3.54 | 410% | 116% | reader-bound | prefilter | 1.000 |
| num OR tag | 1.00% | 200 | 4099.7 | 12852.7 | 3.14x | 3.86 | 1.10 | 4.49 | 2.16 | 5.19 | 3.49 | 416% | 170% | reader-bound | prefilter | 1.000 |
| num OR tag | 2.00% | 400 | 7873.9 | 12207.8 | 1.55x | 2.00 | 1.17 | 2.45 | 2.19 | 2.78 | 3.56 | 430% | 295% | reader-bound | prefilter | 1.000 |
| num OR tag | 5.00% | 1000 | 11960.0 | 6095.1 | 0.51x | 1.18 | 2.60 | 2.22 | 2.75 | 3.76 | 2.91 | 351% | 423% | reader-bound | inline | 1.000 |
| num OR tag | 10.00% | 2000 | 12188.0 | 2717.8 | 0.22x | 1.15 | 5.86 | 2.28 | 6.05 | 3.80 | 7.08 | 218% | 412% | reader-bound | inline | 0.995 |
| num OR tag | 20.00% | 4000 | 12310.3 | 1359.2 | 0.11x | 1.14 | 11.71 | 2.30 | 12.08 | 3.83 | 12.91 | 161% | 406% | reader-bound | inline | 1.000 |
| num OR tag | 30.00% | 6000 | 12407.6 | 871.8 | 0.07x | 1.13 | 18.42 | 2.28 | 18.77 | 3.75 | 20.35 | 144% | 403% | reader-bound | inline | 1.000 |
| num OR tag | 40.00% | 8000 | 12289.8 | 650.6 | 0.05x | 1.13 | 24.50 | 2.30 | 25.19 | 3.77 | 28.83 | 134% | 403% | reader-bound | inline | 1.000 |
| num AND NOT tag | 0.50% | 100 | 2690.6 | 12806.9 | 4.76x | 5.89 | 1.10 | 6.74 | 2.16 | 7.53 | 3.55 | 411% | 176% | reader-bound | prefilter | 1.000 |
| num AND NOT tag | 1.00% | 200 | 4663.8 | 12436.4 | 2.67x | 3.38 | 1.15 | 3.93 | 2.14 | 4.60 | 3.42 | 418% | 277% | reader-bound | prefilter | 1.000 |
| num AND NOT tag | 2.00% | 400 | 8766.2 | 11121.5 | 1.27x | 1.80 | 1.43 | 2.16 | 1.54 | 2.45 | 1.79 | 434% | 439% | reader-bound | prefilter | 1.000 |
| num AND NOT tag | 5.00% | 1000 | 11843.7 | 4390.8 | 0.37x | 1.19 | 3.63 | 2.26 | 3.71 | 3.90 | 3.92 | 333% | 417% | reader-bound | inline | 1.000 |
| num AND NOT tag | 10.00% | 2000 | 12271.5 | 2308.5 | 0.19x | 1.15 | 6.88 | 2.26 | 7.17 | 3.73 | 7.31 | 216% | 414% | reader-bound | inline | 0.995 |
| num AND NOT tag | 20.00% | 4000 | 12285.2 | 1372.5 | 0.11x | 1.14 | 11.63 | 2.30 | 11.82 | 3.80 | 12.01 | 168% | 406% | reader-bound | inline | 1.000 |
| num AND NOT tag | 30.00% | 6000 | 12318.4 | 978.6 | 0.08x | 1.14 | 16.31 | 2.28 | 16.57 | 3.74 | 17.62 | 154% | 404% | reader-bound | inline | 1.000 |
| num AND NOT tag | 40.00% | 8000 | 12237.1 | 816.8 | 0.07x | 1.13 | 19.56 | 2.30 | 19.85 | 3.83 | 21.08 | 143% | 404% | reader-bound | inline | 1.000 |

All latencies are client-observed milliseconds under concurrent load, so they include queueing delay and are expected to be higher than single-query latency.

## Crossover by query shape

| query shape | last selectivity where prefilter wins | first where inline wins |
|---|---|---|
| numeric | 5.00% | 10.00% |
| tag | 2.00% | 5.00% |
| text | 2.00% | 5.00% |
| num AND tag | 2.00% | 5.00% |
| num AND text | 2.00% | 5.00% |
| tag AND text | 2.00% | 5.00% |
| num AND tag AND text | 2.00% | 5.00% |
| num OR tag | 2.00% | 5.00% |
| num AND NOT tag | 2.00% | 5.00% |
