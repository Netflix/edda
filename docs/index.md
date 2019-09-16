# Edda

## Introduction

Operating in the Cloud has its challenges, and one of those challenges is that nothing is static.
Virtual host instances are constantly coming and going, IP addresses can get reused by different
applications, and firewalls suddenly appear as security configurations are updated. At Netflix we
needed something to help us keep track of our ever-shifting environment within Amazon Web Services
(AWS). Our solution is Edda.

## What is Edda?

Edda is a service that polls your AWS resources via AWS APIs and records the results. It allows you
to [quickly search](rest-api.md) through your resources and shows you how they have changed over time.

Previously this project was known within Netflix as Entrypoints (and mentioned in some blog posts),
but the name was changed as the scope of the project grew. Edda, which means "a tale of Norse
mythology", seemed appropriate for the new name, as our application records the tales of [Asgard].

[Asgard]: https://medium.com/netflix-techblog/asgard-web-based-cloud-management-and-deployment-2c9fc4e4d3a1

## Why Did We Create Edda?

### Dynamic Querying

At Netflix we need to be able to quickly query and analyze our AWS resources with widely varying
search criteria. For instance, if we see a host with an EC2 hostname that is causing problems on
one of our API servers then we need to find out what that host is and what team is responsible,
Edda allows us to do this. The APIs AWS provides are fast and efficient but limited in their
querying ability. There is no way to find an instance by the hostname, or find all instances in
a specific Availability Zone without first fetching all the instances and iterating through them.

With Edda's [REST APIs](rest-api.md) we can use matrix arguments to find the resources that we are
looking for. Furthermore, we can trim out unnecessary data in the responses with [Field Selectors].

[Field Selectors]: https://developer.linkedin.com/docs/v1/people/field-selectors

### History and Changes

When trying to analyze causes and impacts of outages we have found the historical data stored in
Edda to be very valuable. Currently AWS does not provide APIs that allow you to see the history of
your resources, but Edda records each AWS resource as versioned documents that can be recalled via
the [REST API](./rest-api.md). The "current state" is stored in memory, which allows for quick
access. Previous resource states and expired resources are stored in MongoDB (by default), which
allows for efficient retrieval. Not only can you see how resources looked in the past, but you can
also [get unified diff output](./rest-api.md#_diff) quickly and see all the changes a resource has
gone through.

## High-Level Architecture

Edda is a Scala application that can both run on a single instance or scale up to many instances
running behind a load-balancer for high availability. The data store that Edda currently supports
is MongoDB, which is also versatile enough to run on either a single instance along with the Edda
service, or be grown to include large replication sets. When running as a cluster, Edda will
automatically select a leader which then does all the AWS polling (by default every 60 seconds)
and persists the data. The other secondary servers will be refreshing their in-memory records (by
default every 30 seconds) and handling REST requests.

Currently only MongoDB is supported for the persistence layer, but we are analyzing alternatives.
MongoDB supports JSON documents and allows for advanced query options, both of which are necessary
for Edda. However, as our previous blogs have indicated, Netflix is heavily invested in Cassandra.
We are therefore looking at some options for advance query services that can work in conjunction
with Cassandra.

Edda was designed to allow for easily implementing custom crawlers to track collections of
resources other than those of AWS.

## Configuration

There are many [configuration](./configuration.md) options for Edda. It can be configured to poll
a single AWS region (as we run it here) or to poll multiple regions. If you have multiple AWS
accounts (i.e. test and prod), Edda can be configured to poll both from the same instance. Edda
currently polls 15 different resource types within AWS. Each collection can be individually
enabled or disabled. Additionally, crawl frequency and cache refresh rates can all be tweaked.
