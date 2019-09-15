# REST API

## Introduction

Resources can be fetched using HTTP GET requests. The output will always be JSON (or JSONP if
[_callback](#_callback) is used). In many cases the results of the GET's will be identical to
an equivalent API call to the AWS service, however Edda will preserve cached copies of the API
calls so users can query what the resource looked like at a specific time, or can quickly scan
through all changes to a resource. Using Matrix Arguments you can find specific resources and
with Field Selectors you can extract just the data elements you are interested.

## General

All APIs might return an error. There will be a non 200 HTTP status code returned with content
formatted as:

```json
{
  "code": 404,
  "message": "not found"
}
```

## Select Matrix Arguments

You can use matrix arguments to query for specific resources. The argument name should map to
the location in the document with a `.` character separating hierarchy levels.

Given the following AWS AutoScalingGroup document structure:

```json
{
  "launchConfigurationName" : "edda-201106231306",
  "instances" : [
    {
      "launchConfigurationName" : "edda-201106231306",
      "instanceId" : "i-0123456789",
      "lifecycleState" : "InService",
      "availabilityZone" : "us-east-1d",
      "healthStatus" : "Healthy"
    } 
  ],
  "availabilityZones" : [
    "us-east-1c",
    "us-east-1d",
    "us-east-1e"
  ],
  "autoScalingGroupName" : "edda"
}
```

You could match it with these any of these queries:

```bash
# set the base url
export ASGS="http://localhost:8080/edda/api/v2/aws/autoScalingGroups"

# find an asg with a launch config
curl "$ASGS;launchConfigurationName=edda-201106231306"

# find all asgs configured for a zone
curl "$ASGS;availabilityZones=us-east-1e"

# find all asgs with at least one instance in a zone
curl "$ASGS;instances.availabilityZone=us-east-1e"
```

Given this AWS Instance document structure:

```json
 {
   "instanceId" : "i-0123456789",
   "publicIpAddress" : "1.2.3.4",
   "tags" : [
     { 
     "key" : "aws:autoscaling:groupName",
     "value" : "edda-v107"
     }
   ],
   "state" : {
     "name" : "terminated",
     "code" : 16
   },
   "securityGroups" : [
     { 
     "groupId" : "sg-0123456789",
     "groupName" : "corp"
     }
   ],
   "instanceType" : "m2.2xlarge",
   "privateIpAddress" : "10.10.10.1",
   "publicDnsName" : "ec2-1-2-3-4.compute-1.amazonaws.com",
   "privateDnsName" : "ip-10-10-10-1.ec2.internal",
   "imageId" : "ami-0123456789",
   "placement" : {
     "tenancy" : "default",
     "availabilityZone" : "us-east-1e",
     "groupName" : ""
   }
 }
```

You could match it with any of these queries:

```bash
# set the base url
export INSTANCES="http://localhost:8080/edda/api/v2/view/instances"

# find an instance with a known publicIpAddress
curl "$INSTANCES;publicIpAddress=1.2.3.4"

# find instances in a given state
curl "$INSTANCES;state.name=terminated"

# find instances given a security group name
curl "$INSTANCES;securityGroups.groupName=corp"

# find instances for a zone
curl "$INSTANCES;placement.availabilityZone=us-east-1e"
```

## Modifier Matrix Arguments

All Modifier Matrix Arguments begin with a `_` to try to avoid conflicts with the Select Matrix
Arguments as described above.

### _all

By default, the APIs only return the most recent documents. If you need to see every document
revision then then you can set `_all`.

Typically this Modifier would be used with [_diff](#_diff) or [_meta](#_meta). The [_all](#_all)
parameter implies [_expand](#_expand).

For example, you can use Field Selectors and `_all` to see all the state changes of a given
instance, and use [_meta](#_meta) to see when those changes happened:

```
curl 'http://localhost:8080/edda/api/v2/view/instances/i-0123456789;_all;_pp;_meta:(stime,ltime,data:(state:(name)))'
```

### _at

The value of this parameter is a timestamp in milliseconds.

You can use `_at` to view a document as it appeared in the past. If a document has changed (asg
has new instances, instance has had eip attached) or is no longer valid (instance was terminated)
then you can use `_at` to see the document as it was in the past.  If you query for a document
that is no longer valid you will get a 410 GONE error when fetched, and you can use `_at` to find
the last seen copy:

```
curl 'http://localhost:8080/edda/api/v2/view/instances/i-0123456789;_pp;_at=1340837213797'
```

### _callback

The value of this parameter is the name of the callback.

This feature is used to provide a jsonp callback. The response `content-type` will be set to
`application/javascript` and the content will be wrapped with the provided callback name.

```
curl 'http://localhost:8080/edda/api/v2/view/instances/i-0123456789;_pp;_at=1340837213797;_callback=mycallback'
```

### _diff

The value of this parameter is an integer representing the number of lines of context you wish to
see.

`_diff` can be used to get a unified diff when several revisions of a document are found. This
is typically used with [_all](#_all), [_since](#_since), [_until](#_until) and [_limit](#_limit).

You can pass a value with the `_diff` argument as the number of lines of context. If you want
the full document, leave a blank value for `_diff`.

For example, to see the changes of a securityGroup over a given time range:

```
curl "http://localhost:8080/edda/api/v2/aws/securityGroups/sg-0123456789;_since=1340398800000;_until=1340402400000;_all;_diff=0"
```

To see the diffs in the last 4 revisions of an instance:

```
curl 'http://localhost:8080/edda/api/v2/view/instances/i-0123456789;_diff=0;_all;_limit=4'
```

### _expand

By default, when you get a collection without specifying a resource id, you will receive an index
listing that contains only get the resource names.  If you want to see the full details of each
resource in the list, add the `_expand` parameter:

```
# index listing
curl 'http://localhost:8080/edda/api/v2/view/instances;_limit=2;_pp'
 
# expanded
curl 'http://localhost:8080/edda/api/v2/view/instances;_limit=2;_pp;_expand'
```

### _limit

The value of this parameter is an integer.

Any time a list is returned, you can restrict the quantity returned with `_limit`. It is
frequently used with [_all](#_all) to see the last `N` revisions of a document. See the
usage in [_diff](#_diff) for an example use case.

### _live

This option should rarely be used. Edda is built on top of a MongoDB datastore, but there is
some in memory caching in the application servers.  There is a possibility for the caches to be
out of sync for brief period of time (cache is refreshed every 60s). If variation in the GET
responses cannot be tolerated, then you can use `_live` to direct the queries directly to
the MongoDB datastore.

### _meta

Edda collects some metadata for the documents it stores.  If you use the `_meta` Modifier
then the related metadata will be returned along the document.  Here are the keys you will see:

* `stime`: Timestamp that Edda detected the document modification.
* `ltime`: Timestamp for when the document was last valid.
* `mtime`: Timestamp of when meta data was modified; it is typically the same as the `stime`.
* `ctime`: Either the create time of the document, or, if the the document has no create or start
time available, then it will be the first time Edda saw the document.
* `tags`: Map of key-value pairs that are used for internal Edda mappings.
* `id`: Primary identifier for the document (instanceId, autoScalingGroupName, etc).
* `data`: The document as stored.
* `_id`: Internal primary key, formatted as `id|stime`, which isused by MongoDB to track the
revisions.

When using `_meta` with Select Matrix Arguments or Field Selectors, the document root changes
and you will need to modify parameter names to match (i.e. add a `data.` prefix when you want
to filter results with `_meta` and Select Matrix Arguments).

```
# find all instances that have ever had an IP and print the instance ids and the tags:
curl 'http://localhost:8080/edda/api/v2/view/instances;publicIpAddress=1.2.3.4;_pp;_since=0;_expand:(instanceId,tags)';

# now make the same request with _meta and select the ltime and stime
# use data.publicIpAddress and sub field selectors data:(instanceId,tags):
curl 'http://localhost:8080/edda/api/v2/view/instances;data.publicIpAddress=1.2.3.4;_pp;_since=0;_expand;_meta:(stime,ltime,data:(instanceId,tags))';
```

### _pp

This will pretty print the response. Along with beautifying the format to make it readable, it
will also translate all the timestamps to more readable `YYYY-MM-DDTHH:mm:ss.SSSZ` time format.

Note that the [jq](https://stedolan.github.io/jq/) CLI tool can be used to provide a similar
capability, except that it will not translate timestamps.

``` 
# without pretty-print
curl "http://localhost:8080/edda/api/v2/aws/volumes/vol-0123456789"

# with pretty-print
curl "http://localhost:8080/edda/api/v2/aws/volumes/vol-0123456789;_pp"
```

### _since

`_since` is the start of a time range.  If [_until](#_until) is not also specified, then the
time range ends at "now".

When `_since` is added to a request, it will show you the most recent revision of all resources
that were valid at some point in that time range.  Note that `_since` does **not** show you
documents "modified since"; it will show any document that was valid during the time frame,
even it that document had no revisions during the period.

If you want to see only documents that were modified, use [_updated](#_updated) along with
`_since`.  If you want to see all the documents alive in a time frame, then use [_all](#_all)
with `_since`, otherwise it will only show you the most recent document found for each resource.

```
# set the base url
export EDDA=http://localhost:8080
 
# show the instance that has an EIP
curl "$EDDA/api/v2/view/instances;publicIpAddress=1.2.3.4"
 
# show all instances that have ever had an EIP
curl "$EDDA/api/v2/view/instances;publicIpAddress=1.2.3.4;_since=0"
 
# show all instances that have had an EIP since June 10th
curl "$EDDA/api/v2view/instances;publicIpAddress=1.2.3.4;_since=1339286400000"
 
# show all instances that had an EIP from June 10th to June 15th
curl "$EDDA/api/v2/view/instances;publicIpAddress=1.2.3.4;_since=1339286400000;_until=1339718400000"
 
# see all revisions that had an EIP from June 10th to June 15th (and select only id, and stime)
curl "$EDDA/api/v2/view/instances;data.publicIpAddress=1.2.3.4;_since=1339286400000;_until=1339718400000;_all;_meta:(stime,id)"
 
# see list of all instances that changed on Jun 11th between 10:00 and 10:01 am:
curl "$EDDA/api/v2/view/instances;_since=1339408800000;_until=1339408860000;_updated"
 
# you can use jshon to turn that list into something processable by the shell
curl "$EDDA/api/v2/view/instances;_since=1339408800000;_until=1339408860000;_updated" | jshon -a -u
 
# and a quick loop will show you all the changes
curl "$EDDA/api/v2/view/instances;_since=1339408800000;_until=1339408860000;_updated" \
| jshon -a -u \
| while read instance; do \
    curl "$EDDA/api/v2/view/instances/$instance;_since=1339408800000;_until=1339408860000;_all;_diff=2"; \
done
```

### _until

The value of this parameter is a timestamp, in milliseconds.

This is the end of a time range started by [_since](#_since).

See the above documentation of [_since](#_since) for examples.

### _updated

This will change the behavior from showing any valid documents in a time range to only showing
documents that have been modified in a time range. You can implement "if modified since" logic
with this to see only changed resources since the last time you polled (useful for periodic
local cache updates). See an example in [_since](#_since).

## Field Selectors

[Field Selectors] can be used to restrict the data returned, and use matrix arguments to filter
the data returned in cases where the resource id is not available or desired.  

Field Selectors syntax must always go at the end of the URI, it follows the form of:

```
:(key,key:(subkey,subkey))
```

Here is an example that filters an AutoScalingGroup to only show the ASG name, instanceIds, and
health status:

```bash
# set the base url
export ASGS="http://localhost:8080/edda/api/v2/aws/autoScalingGroups/edda-v123"

# filter the results to the specified fields
curl "$ASGS;_pp:(autoScalingGroupName,instances:(instanceId,lifecycleState))"
```

```json
{ 
  "autoScalingGroupName" : "edda-v123",
  "instances" : [
    { 
      "instanceId" : "i-0123456789",
      "lifecycleState" : "InService"
    },
    { 
      "instanceId" : "i-012345678a",
      "lifecycleState" : "InService"
    },
    { 
      "instanceId" : "i-012345678b",
      "lifecycleState" : "InService"
    }
  ]
}
```
 
[Field Selectors]: https://developer.linkedin.com/docs/v1/people/field-selector

## Collection APIs

### /api/v2/aws

All APIs under `/api/v2/aws` return the raw (JSON serialized) resources as the Amazon AWS APIs
return.

<table>
    <tr>
        <th>Collection <th>Endpoints <th>Notes
    <tr>
        <td>addresses
        <td>
            <ul>
                <li>GET /api/v2/aws/addresses
                <li>GET /api/v2/aws/addresses;_all
                <li>GET /api/v2/aws/addresses/:name
        <td>
    <tr>
        <td>alarms
        <td>
            <ul>
                <li>GET /api/v2/aws/alarms
                <li>GET /api/v2/aws/alarms;_all
                <li>GET /api/v2/aws/alarms/:name
        <td>
    <tr>
        <td>autoScalingGroups
        <td>
            <ul>
                <li>GET /api/v2/aws/autoScalingGroups
                <li>GET /api/v2/aws/autoScalingGroups;_all
                <li>GET /api/v2/aws/autoScalingGroups/:name
        <td>
    <tr>
        <td>buckets
        <td>
            <ul>
                <li>GET /api/v2/aws/buckets
                <li>GET /api/v2/aws/buckets;_all
                <li>GET /api/v2/aws/buckets/:name
        <td>
    <tr>
        <td>databases
        <td>
            <ul>
                <li>GET /api/v2/aws/databases
                <li>GET /api/v2/aws/databases;_all
                <li>GET /api/v2/aws/databases/:name
        <td>
    <tr>
        <td>iamGroups
        <td>
            <ul>
                <li>GET /api/v2/aws/iamGroups
                <li>GET /api/v2/aws/iamGroups;_all
                <li>GET /api/v2/aws/iamGroups/:name
        <td>
    <tr>
        <td>iamRoles
        <td>
            <ul>
                <li>GET /api/v2/aws/iamRoles
                <li>GET /api/v2/aws/iamRoles;_all
                <li>GET /api/v2/aws/iamRoles/:name
        <td>
    <tr>
        <td>iamUsers
        <td>
            <ul>
                <li>GET /api/v2/aws/iamUsers
                <li>GET /api/v2/aws/iamUsers;_all
                <li>GET /api/v2/aws/iamUsers/:name
        <td>
    <tr>
        <td>iamVirtualMFADevices
        <td>
            <ul>
                <li>GET /api/v2/aws/iamVirtualMFADevices
                <li>GET /api/v2/aws/iamVirtualMFADevices;_all
                <li>GET /api/v2/aws/iamVirtualMFADevices/:name
        <td>
    <tr>
        <td>images
        <td>
            <ul>
                <li>GET /api/v2/aws/images
                <li>GET /api/v2/aws/images;_all
                <li>GET /api/v2/aws/images/:name
        <td>
    <tr>
        <td>instances
        <td>
            <ul>
                <li>GET /api/v2/aws/instances
                <li>GET /api/v2/aws/instances;_all
                <li>GET /api/v2/aws/instances/:name
        <td>
    <tr>
        <td>launchConfigurations
        <td>
            <ul>
                <li>GET /api/v2/aws/launchConfigurations
                <li>GET /api/v2/aws/launchConfigurations;_all
                <li>GET /api/v2/aws/launchConfigurations/:name
        <td>
    <tr>
        <td>loadBalancers
        <td>
            <ul>
                <li>GET /api/v2/aws/loadBalancers
                <li>GET /api/v2/aws/loadBalancers;_all
                <li>GET /api/v2/aws/loadBalancers/:name
        <td>
            To find information about instances behind a load balancer, see the
            /view/loadBalancerInstances api
    <tr>
        <td>reservedInstances
        <td>
            <ul>
                <li>GET /api/v2/aws/reservedInstances
                <li>GET /api/v2/aws/reservedInstances;_all
                <li>GET /api/v2/aws/reservedInstances/:name
        <td>
    <tr>
        <td>scalingPolicies
        <td>
            <ul>
                <li>GET /api/v2/aws/scalingPolicies
                <li>GET /api/v2/aws/scalingPolicies;_all
                <li>GET /api/v2/aws/scalingPolicies/:name
        <td>
    <tr>
        <td>securityGroups
        <td>
            <ul>
                <li>GET /api/v2/aws/securityGroups
                <li>GET /api/v2/aws/securityGroups;_all
                <li>GET /api/v2/aws/securityGroups/:name
        <td>
    <tr>
        <td>snapshots
        <td>
            <ul>
                <li>GET /api/v2/aws/snapshots
                <li>GET /api/v2/aws/snapshots;_all
                <li>GET /api/v2/aws/snapshots/:name
        <td>
    <tr>
        <td>tags
        <td>
            <ul>
                <li>GET /api/v2/aws/tags
                <li>GET /api/v2/aws/tags;_all
                <li>GET /api/v2/aws/tags/:name
        <td>
            The tags api is a bit special. There are no primary keys for tags, there is a unique
            tuple of the tag name, the resource id and the resource type.
    <tr>
        <td>volumes
        <td>
            <ul>
                <li>GET /api/v2/aws/volumes
                <li>GET /api/v2/aws/volumes;_all
                <li>GET /api/v2/aws/volumes/:name
        <td>
</table>

### /api/v2/group

These collections are special and quite different, because they primarily deal with group
memberships (i.e. instances that are members of AutoScalingGroups). There is no corresponding
AWS API that contains all the details. Also we do not store complete history for these APIs.
New document revisions are generated for history any time the membership changes, not when a
particular member changes.  If an instance change IP address, it will be captured and stored
to the data store, however you cannot find out what the previous IP address was with this API,
for that you would need to use `/view/instances`.  Basic interesting details about each member
are captured and stored, but for more information about the members you will need to use the
particular member apis.

An additional key added for each member is `slot`.  Slots are arbitrary numbers ranging from
0 to the size of the group. The slot numbers are assigned to members as soon as they appear, and
the slot ID will never change for a member.  When membership changes (ie old instance dies, new
instance becomes member) then the old slot id will be reassigned to the new member.  This
mechanism is primarily used within Netflix for our Monitoring systems, so they can efficiently
shard the massive amounts of metric data they collect for each instance.

When using time range Modifier Matrix Arguments, the historical members will be merged into a
single resource and the "end" time will be set to the last time the group saw the member.

<table>
    <tr>
        <th>Collection <th>Endpoints <th>Notes
    <tr>
        <td>autoScalingGroups
        <td>
            <ul>
                <li>GET /api/v2/group/autoScalingGroups
                <li>GET /api/v2/group/autoScalingGroups;_all
                <li>GET /api/v2/group/autoScalingGroups/:name
        <td>
</table>
 
### /api/v2/view

The view APIs represent aspects of other APIs, to make the data more usable and accessible.

The `/view/instances` collection is the most commonly used one, since `/aws/instances` really
returns the EC2 reservations.  Since the individual instance data is generally more useful for
us, we pull apart the `/aws/instances` document and store the instance details separated in a
`/view` API.

As described by the AWS SDK `DescribeInstances` documentation:

>The ID of the instance's reservation. A reservation ID is created any time you launch an
>instance. A reservation ID has a one-to-one relationship with an instance launch request,
>but can be associated with more than one instance if you launch multiple instances using
>the same launch request. For example, if you launch one instance, you get one reservation
>ID. If you launch ten instances using the same launch request, you also get one reservation
>ID.

The `/view/simpleQueues` API likewise does not correspond to a single AWS API, it is a merge of
multiple API calls to make the data more useful.

<table>
    <tr>
        <th>Collection <th>Endpoints <th>Notes
    <tr>
        <td>instances
        <td>
            <ul>
                <li>GET /api/v2/view/instances
                <li>GET /api/v2/view/instances;_all
                <li>GET /api/v2/view/instances/:name
        <td>
            The documents from this API are derived from the /aws/instances API, but split
            out to be individually accessible.
    <tr>
        <td>loadBalancerInstances
        <td>
            <ul>
                <li>GET /api/v2/view/loadBalancerInstances
                <li>GET /api/v2/view/loadBalancerInstances;_all
                <li>GET /api/v2/view/loadBalancerInstances/:name
        <td>
            The documents from this API do not directly correspond to any AWS API; it is
            generated based on several AWS API calls.
    <tr>
        <td>simpleQueues
        <td>
            <ul>
                <li>GET /api/v2/view/simpleQueues
                <li>GET /api/v2/view/simpleQueues;_all
                <li>GET /api/v2/view/simpleQueues/:name
        <td>
            <p>The documents from this API do not directly correspond to any AWS API; it is
            generated based on several AWS API calls.</>
            <p>The Approximate* attributes are changing constantly, so Edda will not generate
            document revisions every time the values of an Approximate* key changes.  However
            a change to any other attribute will generate a new document revision.</p>
</table>
