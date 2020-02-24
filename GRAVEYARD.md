# Link to code removed from this repository

- 2020-02-04 (available until [commit `7e60dfc`](https://github.com/mozilla/gcp-ingestion/tree/7e60dfcd2dd8f67ca97e44b42468d8550960906f))
  - Remove patched `WritePartition.java` that limits the maximum number of bytes
    in a single BigQuery load job; Beam 2.19 exposes a configuration parameter
    we now use for the same effect.
- 2020-01-24 (available until [commit `16d7702`](https://github.com/mozilla/gcp-ingestion/tree/16d770233c073af07c9b0f7ca6f9a1b4080d71d3))
  - `HekaReader` and the `heka` input type were removed
  - The `sanitizedLandfill` input format was removed along with AWS dependencies