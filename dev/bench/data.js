window.BENCHMARK_DATA = {
  "lastUpdate": 1781050064546,
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
    ],
    "HLS pipeline benchmarks": [
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
          "id": "e18b2de39ecccb0f3d8b423c3dd5478d2af2a85c",
          "message": "feat: generalize metrics for benchmark (#46)",
          "timestamp": "2026-06-09T19:45:32-04:00",
          "tree_id": "4fd41299cc438db8d0d0ff37622a092947f8339c",
          "url": "https://github.com/NASA-IMPACT/hls-science-container/commit/e18b2de39ecccb0f3d8b423c3dd5478d2af2a85c"
        },
        "date": 1781050063517,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Fmask runtime_seconds [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 288.587,
            "unit": "s"
          },
          {
            "name": "Fmask peak_memory_mb [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 11313.9,
            "unit": "MB"
          },
          {
            "name": "Fmask avg_cpu_percent [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 174.7,
            "unit": "%"
          },
          {
            "name": "LaSRC runtime_seconds [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 20.493,
            "unit": "s"
          },
          {
            "name": "LaSRC peak_memory_mb [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 5129.4,
            "unit": "MB"
          },
          {
            "name": "LaSRC avg_cpu_percent [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 284.6,
            "unit": "%"
          },
          {
            "name": "landsat-ac runtime_seconds [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 377.698,
            "unit": "s"
          },
          {
            "name": "landsat-ac peak_memory_mb [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 11300.2,
            "unit": "MB"
          },
          {
            "name": "landsat-ac avg_cpu_percent [LC08_L1TP_184033_20260103_20260107_02_T1]",
            "value": 165.8,
            "unit": "%"
          },
          {
            "name": "Fmask runtime_seconds [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 110.364,
            "unit": "s"
          },
          {
            "name": "Fmask peak_memory_mb [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 7065.6,
            "unit": "MB"
          },
          {
            "name": "Fmask avg_cpu_percent [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 108.7,
            "unit": "%"
          },
          {
            "name": "LaSRC runtime_seconds [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 56.649,
            "unit": "s"
          },
          {
            "name": "LaSRC peak_memory_mb [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 13142.4,
            "unit": "MB"
          },
          {
            "name": "LaSRC avg_cpu_percent [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 111.4,
            "unit": "%"
          },
          {
            "name": "sentinel-ac runtime_seconds [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 277.211,
            "unit": "s"
          },
          {
            "name": "sentinel-ac peak_memory_mb [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 13086.2,
            "unit": "MB"
          },
          {
            "name": "sentinel-ac avg_cpu_percent [S2B_MSIL1C_20260124T073109_N0511_R049_T38PNC_20260124T092241]",
            "value": 105.6,
            "unit": "%"
          }
        ]
      }
    ]
  }
}