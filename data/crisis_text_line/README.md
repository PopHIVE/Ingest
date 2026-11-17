# crisis_text_line

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

Mirrors the published, already-suppressed `standard/data.csv.gz` from the
**private** [PopHIVE/crisis-text-line](https://github.com/PopHIVE/crisis-text-line)
repository, where the Crisis Text Line extract, cell suppression and percent
calculation happen. Nothing is saved to `raw/`.

## Access

The download needs one of:

- `CTL_READ_TOKEN`: a GitHub fine-grained personal access token with
  **Contents: read** on `PopHIVE/crisis-text-line` only. In CI it is the
  `CTL_READ_TOKEN` repository secret, passed to the build step in
  `.github/workflows/build.yaml`. Locally, put it in `.Renviron`.
- `CTL_LOCAL_REPO`: path to a local clone of `crisis-text-line`. It mirrors
  whatever is checked out there, so make sure the clone is on an up-to-date
  `main`. If both are set, `CTL_READ_TOKEN` is used.

With neither set, `ingest.R` skips and leaves the committed standard file
as is, so `dcf_build()` still runs for people without access. If the token is
set but the download fails (expired/revoked token), the source errors so the
build flags it. Fine-grained tokens expire: renew the secret before the
expiration date.

## Usage

```R
dcf_check()
dcf_process()
```
