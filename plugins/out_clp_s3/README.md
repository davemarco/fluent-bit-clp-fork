# Fluent Bit S3 output plugin for CLP

Fluent Bit output plugin that sends records in CLP's compressed KV-IR format to AWS S3.

### Getting Started

There are three ways to use the plugin:

- [Run the prebuilt image](#run-the-prebuilt-image)
- [Build and run with Docker Compose](#build-and-run-with-docker-compose)
- [Build and run locally](#build-and-run-locally)

#### Run the prebuilt image

The published image bundles the plugin and Fluent Bit. Currently, we only publish a `linux/amd64` image.

Download the sample config and edit it to suit your needs (see [Plugin configuration](#plugin-configuration)):
  ```shell
  curl -fsSL -O https://raw.githubusercontent.com/y-scope/fluent-bit-clp/main/plugins/out_clp_s3/fluent-bit.yaml
  ```

Set up your [AWS credentials](#aws-credentials).

Start the plugin:
  ```shell
  docker run --rm \
    -v "$(pwd)/fluent-bit.yaml:/fluent-bit/etc/fluent-bit.yaml" \
    -v ~/.aws/credentials:/root/.aws/credentials:ro \
    -v clp_disk_buffer:/disk_buffer/ \
    ghcr.io/y-scope/fluent-bit-clp:latest
  ```

The mounts are:
- `fluent-bit.yaml` — your plugin configuration.
- `~/.aws/credentials` — your AWS credentials. Drop this if you use another credential method.
- `clp_disk_buffer` — a persistent volume for the [disk buffer](#disk-buffering), so logs buffered on disk aren't lost when the container stops. Drop this if you disable disk buffering.

#### Build and run with Docker Compose

Clone this repo:
  ```shell
  git clone https://github.com/y-scope/fluent-bit-clp.git
  cd fluent-bit-clp/plugins/out_clp_s3
  ```

Set up your [AWS credentials](#aws-credentials).

Edit [fluent-bit.yaml](fluent-bit.yaml) to suit your needs (see [Plugin configuration](#plugin-configuration)).

Build and run:
  ```shell
  docker compose up
  ```

#### Build and run locally

Clone this repo:
  ```shell
  git clone https://github.com/y-scope/fluent-bit-clp.git
  cd fluent-bit-clp/plugins/out_clp_s3
  ```

Install [go][2], [task][3], and [fluent-bit][4].

Set up your [AWS credentials](#aws-credentials).

Edit [fluent-bit.yaml](fluent-bit.yaml) to suit your needs (see [Plugin configuration](#plugin-configuration)).

Download go dependencies:
  ```shell
  go mod download
  ```

Build the plugin:
  ```shell
  task build
  ```

Run Fluent Bit:
  ```shell
  fluent-bit -e ./out_clp_s3.so -c fluent-bit.yaml
  ```

### AWS Credentials

Credentials are resolved using the [default AWS SDK credential chain][5]. To assume a role, add `role_arn` to [fluent-bit.yaml](fluent-bit.yaml):
```yaml
role_arn: arn:aws:iam::000000000000:role/accessToMyBucket
```

### Plugin Configuration

The plugin is configured by editing your `fluent-bit.yaml`. If your logs are JSON, use the [Fluent Bit JSON parser][1] on your input. Below is a simple example:

```yaml
pipeline:
  inputs:
    - name: tail
      path: /var/log/app.json
      tag: app.json
      parser: json

  outputs:
    - name: out_clp_s3
      match: "*"
      s3_bucket: myBucket
```

The output supports the following options:

| Key                 | Description                                                                                                  | Default           |
|---------------------|--------------------------------------------------------------------------------------------------------------|-------------------|
| `s3_region`         | The AWS region of your S3 bucket                                                                             | `us-east-1`       |
| `s3_bucket`         | S3 bucket name. Just the name, no aws prefix necessary.                                                      | `None`            |
| `s3_bucket_prefix`  | Bucket prefix path                                                                                           | `logs/`           |
| `role_arn`          | ARN of an IAM role to assume                                                                                 | `None`            |
| `id`                | Name of output plugin                                                                                        | Random UUID       |
| `use_disk_buffer`   | Buffer logs on disk prior to sending to S3. See [Disk Buffering](#disk-buffering) for more info.             | `TRUE`            |
| `disk_buffer_path`  | Directory for disk buffer. Path should be unique for each output.                                            | `./disk_buffer/`  |
| `upload_size_mb`    | Set upload size in MB. Size refers to the compressed size.                                                   | `16`              |
| `timeout`           | Upload timeout if upload size is not met. See [time.ParseDuration][6] for valid duration strings (e.g. s, m, h). | `15m`             |

#### Disk Buffering

The output plugin receives raw logs from Fluent Bit in small chunks and accumulates them in a compressed
buffer until the upload size or timeout is reached before sending to S3.

With `use_disk_buffer` set, logs are stored on disk as KV-IR and Zstd compressed KV-IR. On a graceful shutdown
or abrupt crash, stored logs will be sent to S3 when Fluent Bit restarts. For an abrupt crash, there is
a very small chance of data corruption if the plugin crashes mid-write. The upload index restarts on
recovery.

With `use_disk_buffer` off, logs are stored in memory as Zstd compressed KV-IR. On a graceful shutdown, the
plugin will attempt to upload any buffered data to S3 before Fluent Bit terminates it. On an abrupt
crash, in-memory data is lost.

### S3 Objects

Each upload will have a unique key in the following format:
```
<FLUENT_BIT_TAG>_<INDEX>_<UPLOAD_TIME_RFC3339>_<ID>.zst
```
The index starts at 0 and is incremented after each upload. The Fluent Bit tag is also attached to the
object using the tag key `fluentBitTag`.

[1]: https://docs.fluentbit.io/manual/data-pipeline/parsers/json
[2]: https://go.dev/doc/install
[3]: https://taskfile.dev/installation
[4]: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit
[5]: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html#specifying-credentials
[6]: https://pkg.go.dev/time#ParseDuration
