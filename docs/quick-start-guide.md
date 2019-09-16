## Git Fork & Clone

If you have a fork of the Edda repo and you see the following error when trying to start sbt:

```
java.lang.RuntimeException: Setting value cannot be null: {file:.../edda/}/*:version
```

Then you need to add the upstream repository and fetch it, so that you have tag data available:

```bash
git remote add upstream https://github.com/Netflix/edda.git
git fetch upstream
```

## AWS Credentials

Proper read-only credentials are required to allow Edda to crawl AWS resources in your account.

The fastest way to get started is to create a new set of PowerUser credentials in the [Identity
and Access Management] console. The downside to this approach is that you need to be careful
about leaking these credentials, since they are long-lived and have the ability to modify items
in your account. Do not use this approach to run Edda in your AWS account; rather, you should
use instance credentials, which are rotated regularly.

The best way to obtain keys for local testing is to create an EddaInstanceProfile IAM role,
create an Edda role and launch an instance with the instance profile. This will provide
credentials which only have permission to read data from AWS APIs and are only valid for a
few hours, which helps reduce the risk of leaking them. See [AWS Permissions] for more details
on configuring these IAM roles. When you are ready to run Edda in your AWS account, this is
the approach you should use for providing credentials.

[Identity and Access Management]: https://aws.amazon.com/iam/
[AWS Permissions]: ./aws-permissions.md

## Run Edda Locally

Using established AWS credentials:

```bash
export AWS_ACCESS_KEY_ID=yourAccessKey
export AWS_SECRET_KEY=yourSecretKey
```

Using time-limited AWS instance credentials:

```bash
# gather credentials from an AWS instance
curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/EddaInstanceProfile

# using the result of the previous step on your machine
CREDENTIALS="..."
export AWS_ACCESS_KEY_ID=$(echo "CREDENTIALS" |jq -r '.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo "CREDENTIALS" |jq -r '.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo "CREDENTIALS" |jq -r '.Token')
```

The default values in `edda.properties` will use a stack name of `dev` and run against the
`us-east-1` region:

```bash
sbt
> jetty:start
```

You should see Edda start and begin processing all the resources under your AWS account for
the region you specified. The output will be pretty verbose, it will log for each new record
it sees, each record that is no longer active, and it will log unified diff output for all
changes that are detected. The first time Edda is run it can take several minutes to insert
all of the AWS resources into S3, depending on how many resources you have.

The Jetty server will be listening at `http://localhost:8080`.

## API Testing

You can start playing with the [REST](./rest-api.md) APIs on your running Edda server. The
[jq](https://stedolan.github.io/jq/) CLI tool is recommended for working JSON API data.

Set the base URL for your local Edda instance:

```bash
export EDDA=http://localhost:8080
```

Find out how many instances you have:

```bash
curl $EDDA/api/v2/view/instances |jq 'length'
```

Make the output easier to read with either jq or the pretty-printer. The jq tool is fast and
allows you to filter the JSON response; the pretty-printer will translate timestamps into
readable strings. Quotes are necessary on the shell, due to the use of the `;` character as
a URL separator.

```sh
curl $EDDA/api/v2/view/instances |jq .
curl "$EDDA/api/v2/view/instances;_pp"
```

Find an instance with a known `publicIpAddress`:

```sh
curl "$EDDA/api/v2/view/instances;publicIpAddress=1.2.3.4"
```

See the details of that instance:

```sh
curl $EDDA/view/instances/i-012345678a |jq .
```

Use [Field Selectors](./rest-api.md#field-selectors) to pull out the instance `state` and
`privateIpAddress`:

```sh
curl "$EDDA/view/instances/i-012345678a;_pp:(state:(name),privateIpAddress)"
```

## Build Edda Artifact

Build Edda, run tests and produce a war file:

```
make build
make war
```

You can find the war file in the following location:

```bash
target/scala-2.11/edda_2.11-3.0.0-SNAPSHOT.war
```

This artifact is suitable for deployment with Tomcat or similar servlet containers in your AWS
account.

## Configure Edda

The defaults should work for most simple cases.

See the [Configuration](./configuration.md) page for details on the various configuration options.

## Further Reading

* Continue experimenting with the [REST API](./rest-api.md) and matrix arguments.
* [Configuration](./configuration.md) options are available to customize the Edda service.
