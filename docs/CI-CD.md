[< Back to README](../README.md)

# GitHub Actions

The CI/CD workflow can be triggered in multiple ways:

## 1. Tag Push (Release)

Push a tag to trigger a full release build with GitHub Release artifacts:

```sh
# add and commit local changes
git add .
git commit -m "commit msg"

# push local changes to remote
git push

# add local tag
git tag -a v0.1.0 -m "v0.1.0"

# push local tag to remote
git push origin --tags
```

To delete and recreate tags:

```sh
# delete local tag
git tag --delete v0.1.0

# delete remote tag
git push origin --delete v0.1.0
```

## 2. Manual Trigger (GitHub UI)

Trigger a build manually from the GitHub Actions web interface (build-only, no releases):

1. Go to the [Actions tab](https://github.com/curtisalexander/readstat-rs/actions)
2. Select the **readstat-rs** workflow
3. Click **Run workflow**
4. Optionally specify:
   - **Version string**: Label for artifacts (default: `dev`)

:memo: Manual triggers only build artifacts and do not create GitHub releases. To create a release, use a [tag push](#1-tag-push-release).

## 3. API Trigger (External Tools)

Trigger builds programmatically using the GitHub API. This is useful for automation tools like Claude Code.

### Using `gh` CLI

```sh
# Trigger a build
gh api repos/curtisalexander/readstat-rs/dispatches \
  -f event_type=build

# Trigger a build with custom version label
gh api repos/curtisalexander/readstat-rs/dispatches \
  -f event_type=build \
  -F client_payload='{"version":"test-build-123"}'
```

### Using `curl`

```sh
curl -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/curtisalexander/readstat-rs/dispatches \
  -d '{"event_type": "build", "client_payload": {"version": "dev"}}'
```

## 4. Claude Code Integration

To have Claude Code trigger a CI build, use this prompt:

> Trigger a CI build for readstat-rs by running: `gh api repos/curtisalexander/readstat-rs/dispatches -f event_type=build`

## Event Types

Repository dispatch event types for API triggers:

| Event Type | Description |
|------------|-------------|
| `build`    | Build all targets and upload artifacts |
| `test`     | Same as `build` (alias for clarity) |
| `release`  | Same as `build` (reserved for future use) |

:memo: API triggers only build artifacts and do not create GitHub releases. To create a release, use a [tag push](#1-tag-push-release).

## Artifacts

All builds (regardless of trigger method) upload artifacts that can be downloaded from the workflow run page. Artifacts are retained for the default GitHub Actions retention period.
