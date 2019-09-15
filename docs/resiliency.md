The [Quick Start Guide](./quick-start-guide.md) shows how to quickly get started running Edda,
however you will probably note it is far from a resilient deployment. This document describes
how Edda is run in the production environment at Netflix.

The Edda deployment is comprised of two logical clusters: one for the Edda frontend and one for the
MongoDB database. Two different AMI's have been created: one for the Edda frontend and one for the
MongoDB install.

For the Edda frontend, the typical setup is four instances under one auto-scaling group (ASG).
The ASG is configured to run out of two availability zones (AZ), so there will be two instances
running in each AZ. Then there is an Elastic Load Balancer (ELB) that routes traffic to each of
the instances.  This configuration allows for Edda to continue operating even during a complete
AZ outage. Two instances per AZ are run because the ELB's can have issues when all instances
within a single AZ go down, so it is desirable to have at least one instance in an AZ after
things like [ChaosMonkey] attacks. In some smaller regions where the risk of Edda becoming
unavailable during an AZ outage is more acceptable then the setup can be just two instances in
one AZ to save on the costs. In larger regions, the Edda frontend clusters can be scaled up to
six or eight instances to handle higher HTTP request load.

Edda is designed to have one 'leader' per cluster. The leader will do all the AWS crawling, and
update the datastore, and the others will simply handle the http request traffic. Edda will
automatically pick a leader among the cluster and it currently uses MongoDB to negotiate
leadership. Once the cluster is set up, it should take care of itself for the most part, leaders
will automatically be reassigned if they become non-responsive. If an instance starts to have
problems it is easiest to just terminate it and let the ASG recreate it.

For MongoDB it is a bit more complicated. It is run with a [replica set] of three instances.

Basically MongoDB operates fine when one of the instances goes down, but when two go down (leaving
only the PRIMARY), then it stops accepting writes. The cluster is setup in AWS to minimize risk
of multiple instances being down at once. This means that each of the three instances needs to be
in a different AZ.  For this, create three different ASGs each configured to run one instance in
one AZ (all different AZ's). On top of the ASGs, use an Elastic IP (EIP) for each instance running
MongoDB, so that when an instance comes up, it will be assigned a known IP.  The EIP is important
since the MongoDB replset membership is specified by the IP and hostname, so those need to be
constant for the deployment. You might be able to use ELBs (one for each ASG) instead of EIPs,
which might be a bit easier to manage, but it is untried and we don't know if it will work.
Assigning an EIP to an instance is a manual process so if you have to rebuild an instance then
you have to ssh into the new instance and reassign the EIP (Netflix has an internal tool that
does this, but unfortunately it is not open source).

For backing up MongoDB, there is nothing as fancy as Priam. The easiest thing is to take a daily
backup via a cron job. The backup is basically, mongodump, tar, and push to S3. Although to use
the backups that would require losing all three MongoDB instances, in three different AZs, which
is unlikely. So short of user error, (i.e. someone accidentally deleting all the MongoDB ASGs), I
would not expect to ever need a backup.

Of course at Netflix, [Asgard] is used, so it makes most of this pretty easy. The hardest part
would likely be getting your AMI's set up and Netflix has a tool that should help with that
which is being open sourced soon.

[ChaosMonkey]: https://medium.com/netflix-techblog/netflix-chaos-monkey-upgraded-1d679429be5d
[replica set]: http://docs.mongodb.org/manual/tutorial/deploy-replica-set/
[Asgard]: https://medium.com/netflix-techblog/asgard-web-based-cloud-management-and-deployment-2c9fc4e4d3a1
 