# AdaptiHound-SE

> **Use with [AdaptiHound-BOF](https://github.com/s3c4ct0r/AdaptiHound-BOF)**

A server-side Adaptix Service Extender that combines a completed AdaptiHound multipart AD collection into one ZIP in Downloads. Start it from the context menu of any collection part or its manifest; processing runs on the C2 server and does not submit an agent task.

The merged ZIP contains the original complete JSON files, including their original names and repeated graph records. JSON files from different parts are retained separately, so one dataset can have several entries in the resulting ZIP. Import the resulting ZIP into BloodHound CE through its native AD importer as a single upload; no custom graph definition is needed.

## Project structure

```text
cmd/adaptihound-se/  Plugin entry point, collection discovery and cleanup
internal/merge/     Manifest validation and streaming ZIP assembly
ax_config.axs       Client context menu
config.yaml         Service configuration
Makefile            Build and checks
dist/               Created by make; plugin and installation files
```

## Build and install

The Go module requires Go 1.25.4 or newer and uses `github.com/Adaptix-Framework/axc2 v1.2.0`. Build the plugin with the **same Go toolchain, operating system, architecture, build experiments and shared dependency versions as the teamserver**. The Makefile defaults to `GOEXPERIMENT=jsonv2,greenteagc`; override it to match your server.

The reference Adaptix build does not use `-trimpath`, so shared dependency source paths also affect Go's package fingerprints. Match the server build's absolute module-cache path as well as its dependency versions. The reference Docker build uses `GOMODCACHE=/go/pkg/mod`; use that same path inside the plugin's build container to avoid an `axc2` package version mismatch.

Go plugins require cgo support in both the teamserver and plugin builds. Build with `CGO_ENABLED=1` and an available C compiler.

```sh
cd AdaptiHound-SE
CGO_ENABLED=1 make check
CGO_ENABLED=1 make
```

To select the server's compiler or experiments explicitly:

```sh
CGO_ENABLED=1 make GO=/path/to/server/go GOEXPERIMENT=jsonv2,greenteagc
```

For a server built without Go experiments, use `make GOEXPERIMENT=`. The same overrides apply to `make check`.

Build on Linux for a Linux teamserver, or inside its matching build container. A macOS build is for a macOS host only.

Copy these three files from `dist/` into one directory on the server:

- `AdaptiHound-SE.so`
- `config.yaml`
- `ax_config.axs`

`make clean` removes the generated `dist/` directory. The source configuration and AXS script remain in the project root.

In the Adaptix checkout used by this project, runtime service loading is disabled. Append the installed configuration's absolute path to the existing `Teamserver.extenders` list in the server profile, preserving the other entries:

```yaml
Teamserver:
  extenders:
    # Keep the existing extenders here.
    - "/opt/adaptix/extenders/AdaptiHound-SE/config.yaml"
```

Restart the teamserver, then reconnect the client to receive the service menu from `ax_config.axs`. Importing the script into the client alone does not load the server plugin. Restart the teamserver after replacing the plugin binary as well; Go caches loaded plugins in the running process.

The internal service ID is `AdaptiHoundSE` because Adaptix allows only letters and numbers in service identifiers. Menus and dialogs use `AdaptiHound-SE`. Keep `service_config` empty.

The server needs write access to the extension directory to create its private `.work` directory. It attempts to remove temporary ZIP files after publication or a handled failure; interrupted operations can leave files in `.work`. Allow disk space for the temporary merged ZIP and the new Downloads copy while the source parts are still present.

## Use

1. Finish a multipart AD collection, for example `adaptihound ad --collection full --memory-auto`.
2. Wait until the collection's files are fully received in Downloads.
3. Select exactly one of its finished ZIP or JSON parts, or its `.manifest.json` file.
4. Choose **AdaptiHound-SE: Merge parts** in the context menu.
5. When the completion dialog appears, download the resulting ZIP from Downloads. Its name is derived from the manifest: `custom.manifest.json` becomes `custom.zip`, preserving the collector's timestamp and optional prefix without adding a merge suffix.

The result always uses a `.zip` extension. With the collector's `--random-filenames` option, the random extension remains part of the stem: `custom.random.manifest.json` becomes `custom.random.zip`.

The menu is registered for `beacon` and `gopher` downloads. The server automatically finds the selected part's manifest using the same agent and exact collection filename prefix. Custom collector prefixes are supported; keep the original part and manifest names. For example, `custom-part000001.zip` belongs to `custom.manifest.json`. Duplicate matching manifests are rejected.

If the manifest or any part is missing or still transferring, retry the action after the transfer completes. An empty completed collection has no parts; select its manifest instead.

After the merged ZIP is verified in Downloads and its publication receipt is saved, the service deletes that collection's source ZIP or JSON parts from Downloads and the server's download storage, including the part selected for the action. The manifest is deleted last, after all parts have been removed and their cleanup status saved. The merged ZIP and other collections remain available.

Cleanup is not atomic, and Adaptix may finish deletions asynchronously. If deletion fails or finishes only partially, the service reports a warning and leaves the verified merged ZIP available. While the original manifest remains available in Downloads and on disk, repeat its action or that of a remaining part to reuse the existing result and retry cleanup. Publication receipts in the teamserver database preserve this behavior across service reloads.

Keep the merged ZIP if you need the collected data. Once the source parts have been deleted, recreating a removed merged ZIP requires those original parts again.

The service processes one merge at a time. It has no background scanner or event hooks; it acts on the collection selected through a part or manifest only.

## Validation and supported formats

Supported input is the collector's complete `AdaptiHoundParts` version 1 manifest with either ZIP parts or `--no-zip` JSON parts containing BloodHound v6 AD data. These are independent collection parts, not split ZIP volumes. Empty completed collections produce an empty ZIP.

Ordinary single-archive AD output does not need merging. FreeIPA OpenGraph output uses its separate graph definition and import workflow; this extension handles AD collections only.

Before a result is published, the service checks:

- Manifest completion, format, naming and record accounting.
- Each part's agent, exact name, index, completed transfer and on-disk size.
- The manifest's rolling CRC32 across all source parts in index order.
- ZIP entry names, compression methods and uncompressed CRC32.
- JSON syntax, actual record counts, dataset types, version and collection methods.
- Per-dataset and overall record totals against the manifest.

Incomplete collections, missing or duplicate parts, mixed runs, malformed JSON, mismatched counts and corrupted ZIP data are rejected. Password-protected ZIP parts are currently unsupported; use an unencrypted collection for this extension. Decrypting existing parts changes the source bytes and invalidates their original rolling checksum.

The manifest is limited to 64 KiB, and each input part must be a nonempty file no larger than 2,147,483,647 bytes. Each ZIP part must contain exactly one JSON entry using Store or Deflate compression. JSON size and record limits are checked against the manifest.

ZIP entries are decompressed for streamed validation, then their compressed bytes are copied without recompression. JSON input parts are compressed into separate entries. Output ZIP64 support allows the combined archive to exceed the BOF's per-download size limit. Publishing uses chunks of at most 1 MiB.

The complete graph is not loaded into memory. Memory use depends on buffers, the largest JSON scalar, part metadata and the server's Downloads list. The collector's `--memory-auto` and `--max-mib` options do not limit this server-side process.

Before source cleanup starts, the service verifies the finished output through the teamserver API, checks its size and SHA256, and saves its publication receipt. A failure before cleanup begins leaves the source parts untouched. A failed running publication is canceled; an invalid newly finished output is submitted for deletion. If saving the cleanup status fails after deletion, a warning is shown; the earlier receipt allows a repeated request to reconcile the status while retaining the merged ZIP.

## Checks

```sh
make check
```

This runs `go vet` for the plugin and merger packages.

The plugin follows Adaptix's [Service Extender interface](https://adaptix-framework.gitbook.io/adaptix-framework/development/extenders/service-extender); its API adapter matches the checked-out server sources.
