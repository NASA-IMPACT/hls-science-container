window.BENCHMARK_DATA = {
  "lastUpdate": 1781045907985,
  "repoUrl": "https://github.com/NASA-IMPACT/hls-science-container",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "ceholden@users.noreply.github.com",
            "name": "Chris Holden",
            "username": "ceholden"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c64e6ac56c0637ad2f4b23dc49839a5a306d83e2",
          "message": "fix: Landsat local granule MTL path discovery (#45)",
          "timestamp": "2026-06-09T18:00:33-04:00",
          "tree_id": "7e1f960368a1931ab3d02fa639e38b9ee0870c22",
          "url": "https://github.com/NASA-IMPACT/hls-science-container/commit/c64e6ac56c0637ad2f4b23dc49839a5a306d83e2"
        },
        "date": 1781045907360,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_landsat_ac.py::test_l30_ac[LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 0.0027017211087673318,
            "unit": "iter/sec",
            "range": "stddev: 0",
            "extra": "mean: 370.134429033 sec\nrounds: 1"
          },
          {
            "name": "tests/benchmarks/test_sentinel.py::test_s30[S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 0.003595633398714908,
            "unit": "iter/sec",
            "range": "stddev: 0",
            "extra": "mean: 278.11511606199997 sec\nrounds: 1"
          }
        ]
      }
    ]
  }
}