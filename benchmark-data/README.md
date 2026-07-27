# Local benchmark data

Place the generated synthetic benchmark files in this directory:

```text
benchmark-data/readstat_benchmark_v1.sas7bdat
benchmark-data/readstat_benchmark_v1_manifest.txt
```

The generated files are intentionally ignored by Git. Validate them and preview
the GitHub release from the repository root:

```bash
./scripts/publish-benchmark.sh
```

After reviewing the validation output, publish the immutable
`benchmark-data-v1` release explicitly:

```bash
./scripts/publish-benchmark.sh --publish
```

Do not force-add benchmark binaries to Git or Git LFS. The publication script
uploads the SAS file, manifest, and generated SHA-256 sidecar as GitHub Release
assets.
