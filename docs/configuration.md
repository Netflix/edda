# Configuration

Edda Configuration is done via an [edda.properties] file located at the server root. The file
name and location can be overridden with the system property `-Dedda.properties=file-path`. The
linked file shows the default values provided with the server.

In the documentation below, `$account` refers to any value found in the `edda.accounts` property.
If `edda.accounts` is unset then the configuration options without `$account` are relevant and
configuration options like `edda.$accounts.setting` will be read as `edda.setting`. 

`$collectionName` refers to any of the collections that Edda crawls.

[edda.properties]: https://github.com/Netflix/edda/blob/master/src/main/resources/edda.properties

## General Options

### edda.accounts

Edda can be configured to poll collections for any number of (comma separated) accounts. Per
account configuration options are available below. The default is unset.

### edda.region

This sets the region for the collections that Edda will be polling. For AWS this setting will
determine which [AWS Endpoint](https://docs.aws.amazon.com/general/latest/gr/rande.html) Edda
communicates with. The default is unset.

### edda.$account.region

See [edda.region](#eddaregion). If multiple accounts are being used then you can set the region
per account. This could be useful it you intend to poll multiple regions from a single Edda
instance. For example, if `edda.accounts=prod.us-east-1,prod.eu-west-1`, then you would want to
set the region specifically for each account as:

```
edda.prod.us-east-1.region=us-east-1
edda.prod.eu-west-1.region=eu-west-1
```

## AWS Options

### edda.aws.accessKey

The access key for AWS account. If unset Edda will attempt to access the AWS Account using the
[Default AWS Credential Provider Chain].

[Default AWS Credential Provider Chain]: http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
 
### edda.$account.aws.accessKey

See [edda.aws.accessKey](#eddaawsaccesskey). Allow for per-account AWS credential settings.

### edda.aws.secretKey

The secret key for AWS account. If `accessKey` is set, then `secretKey` must also be set.

### edda.$account.aws.secretKey

See [edda.aws.secretKey](#eddaawssecretkey). Allow for per-account AWS credential settings.

## Collection Options

### edda.collection.cache.refresh

The time period in milliseconds for how frequent secondary edda servers should reload their
in-memory cache of "live" resources. Not used if Edda running in stand-alone mode. Default
value is `30000` (30 seconds).

### edda.collection.$account.cache.refresh

See [edda.collection.cache.refresh](#eddacollectioncacherefresh). If you want to change the
cache refresh rate for a specific account you can set this.  

### edda.collection.$account.$collectionName.cache.refresh

See [edda.collection.cache.refresh](#eddacollectioncacherefresh). If you want to change the
cache refresh rate for a specific account and specific collection you can set this. You might
want to increase the default value if a collection is especially large, or decrease the value,
if the collection is small.

### edda.collection.refresh

The time period in milliseconds for how frequent the primary Edda servers should crawl the AWS
APIs looking for changed resources. Default value is `60000` (1 minute).

### edda.collection.$account.refresh

See [edda.collection.refresh](#eddacollectionrefresh). If you want change the crawl frequency
for a specific account, you can change this value.

### edda.collection.$account.$collectionName.refresh

See [edda.collection.refresh](#eddacollectionrefresh). If you want change the crawl frequency
for a specific account and specific collection, you can change this value.

### edda.collection.jitter.enabled

This boolean value is used to ease the load on your data store upon startup. The default value
is `false`. The jitter cases the initial load of the collection to be staggered randomly over a
time period of 2X the value of `edda.collection.cache.refresh`. You might want to set this to
`true` for development or if you collections are small enough that the data store has no problem
loading them all simultaneously.

### edda.collection.$account.enabled

You can disable all collections for a specific account by setting this value to `false`. The
default value is `true`.

### edda.collection.$account.$collectionName.enabled

You can disable a specific collection for an account by setting this value to `false`. The
default value is `true`.

## Crawler Options

### edda.bean.argPattern

Edda polls some API's via java client libraries which return Java Beans. We serialize the Beans
to a JSON blob before persisting the data to the data store. This setting is a regular express
that determines which properties of the bean to serialize. The default value is `[^a-zA-Z0-9_]`.
Any Bean properties not matching this regular expression will be ignored.

### edda.crawler.aws.suppressTags

AWS provides the ability to add custom tags to many resources that Edda will poll. Sometimes those
tags are frequently changing (like tags with timestamp values) which will cause Edda to persist a
ton of data that is of little use. If there are tags that you need Edda to ignore you can set this
property to a comma separated list of tag names where the value will be automatically replaced with
`[EDDA_SUPPRESSED]`. The default value is unset.

### edda.crawler.abortWithoutTags

AWS APIs for resources that support tagging have an odd quirk: if Amazon is experiencing load
problems with their API servers, then they will silently drop all tag information from the
responses. Until this problem can be fixed by AWS we have this special setting to allow us to
handle this degraded response.  If you routinely use tags on your AWS resources you should set
this value to `true`, which will cause crawl results that are missing any tags to be ignored.
The default value is `false`.

### edda.crawler.$account.abortWithoutTags

See [edda.crawler.abortWithoutTags](#eddacrawlerabortwithouttags). Apply setting to a specific
account.

### edda.crawler.$account.$collectionName.abortWithoutTags

See [edda.crawler.abortWithoutTags](#eddacrawlerabortwithouttags). Apply setting to a specific
account and specific collection.

### edda.crawler.$account.enabled

Boolean value to enable or disable crawlers for an account.  The default value is `true`.

### edda.crawler.$account.$collectionName.enabled

Boolean value to enable or disable a crawler for a specific account and specific collection. The
default value is `true`.

## Elector Options

Elector options control how leader election is done in Edda.

### edda.elector.refresh

The time period in milliseconds for how frequently an election should be run to determine a new
leader. The default value is `10000` (10 seconds).

### edda.elector.mongo.collectionName

The Mongo Elector uses MongoDB's atomic write capabilities to determine leadership. For this
to work every Edda instance in a cluster will attempt to write to a single record in a specific
collection. This values lets you customize the collection name. The default value is
`sys.monitor`.

### edda.elector.mongo.leaderTimeout

If Mongo Elector is being used, leaders that have not updated MongoDB within a timeout period will
automatically loose leadership and a new leader will be chosen. The default value is `30000` (30
seconds). We recommend the value be 3X the value of `edda.elector.refresh`.

### edda.elector.mongo.uniqueEnvName

The Mongo Elector will use a unique id to identify each candidate in the election and it gets
this unique id from an environment variable. The default value is to use the AWS Instance ID
for a unique name, and that value is assumed to be in the `EC2_INSTANCE_ID` environment variable.
If the unique name is in a different environment variable then change this setting.

## Mongo Options

Options for how to connect to MongoDB. You can customize the connections per account or even per
collection to be able to spread out the load on MongoDB between more instances.

### edda.mongo.address

The network `address:port` for where the MongoDB server is running. The default is
`localhost:27017`. If you are running a MongoDB replication set, then set this value
to a comma separated list of all the addresses of the replication set.

### edda.mongo.$account.address

See [edda.mongo.address](#eddamongoaddress).  Set the address for a specific account.

### edda.mongo.$account.$collectionName.address

See [edda.mongo.address](#eddamongoaddress).  Set the address for a specific account and collection.

### edda.mongo.database

Specify the name of the database in the MondoDB server to use. The default value is `edda`.

### edda.mongo.$account.database

See [edda.mongo.database](#eddamongodatabase). Set the database name to use for a specific account.

### edda.mongo.$account.$collectionName.database

See [edda.mongo.database](#eddamongodatabase). Set the database name to use for a specific account
and collection.

### edda.mongo.user

Specify the user name to use when connecting to the MongoDB server. The default is unset.

### edda.mongo.$account.user

See [edda.mongo.user](#eddamongouser). Set the user for a specific account.

### edda.mongo.$account.$collectionName.user

See [edda.mongo.user](#eddamongouser). Set the user for a specific account

### edda.mongo.password

Specify the password to use when connecting to the MongoDB server. The default is unset.

### edda.mongo.$account.password

See [edda.mongo.password](#eddamongopassword). Set the password for a specific account.

### edda.mongo.$account.$collectioName.password

See [edda.mongo.password](#eddamongopassword). Set the password for a specific account and
collection.