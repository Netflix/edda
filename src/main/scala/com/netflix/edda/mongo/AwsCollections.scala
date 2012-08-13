package com.netflix.edda.mongo

import com.netflix.edda.Elector

import com.netflix.edda.aws.AddressCrawler
import com.netflix.edda.aws.AutoScalingGroupCrawler
import com.netflix.edda.aws.ImageCrawler
import com.netflix.edda.aws.LoadBalancerCrawler
import com.netflix.edda.aws.InstanceHealthCrawler
import com.netflix.edda.aws.LaunchConfigurationCrawler
import com.netflix.edda.aws.ReservationCrawler
import com.netflix.edda.aws.InstanceCrawler
import com.netflix.edda.aws.SecurityGroupCrawler
import com.netflix.edda.aws.SnapshotCrawler
import com.netflix.edda.aws.TagCrawler
import com.netflix.edda.aws.VolumeCrawler
import com.netflix.edda.aws.BucketCrawler

import com.mongodb.DB

class AddressCollection(
    db: DB,
    crawler: AddressCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.addresses"),
    "aws.addresses",
    crawler,
    elector
) {}

class AutoScalingGroupCollection(
    db: DB,
    crawler: AutoScalingGroupCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.autoScalingGroups"),
    "aws.autoScalingGroups",
    crawler,
    elector
) {}

class ImageCollection(
    db: DB,
    crawler: ImageCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.images"),
    "aws.images",
    crawler,
    elector
) {}

class LoadBalancerCollection(
    db: DB,
    crawler: LoadBalancerCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.loadBalancers"),
    "aws.loadBalancers",
    crawler,
    elector
) {}

class loadBalancerInstanceCollection(
    db: DB,
    crawler: InstanceHealthCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("view.loadBalancerInstances"),
    "view.loadBalancerInstances",
    crawler,
    elector
) {}

class LaunchConfigurationCollection(
    db: DB,
    crawler: LaunchConfigurationCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.launchConfigurations"),
    "aws.launchConfigurations",
    crawler,
    elector
) {}

class ReservationCollection(
    db: DB,
    crawler: ReservationCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.instances"),
    "aws.instances",
    crawler,
    elector
) {}

class InstanceCollection(
    db: DB,
    crawler: InstanceCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("view.instances"),
    "view.instances",
    crawler,
    elector
) {}

class SecurityGroupCollection(
    db: DB,
    crawler: SecurityGroupCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.securityGroups"),
    "aws.securityGroups",
    crawler,
    elector
) {}

class SnapshotCollection(
    db: DB,
    crawler: SnapshotCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.snapshots"),
    "aws.snapshots",
    crawler,
    elector
) {}

class TagCollection(
    db: DB,
    crawler: TagCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.tags"),
    "aws.tags",
    crawler,
    elector
) {}

class VolumeCollection(
    db: DB,
    crawler: VolumeCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.volumes"),
    "aws.volumes",
    crawler,
    elector
) {}

class BucketCollection(
    db: DB,
    crawler: BucketCrawler,
    elector: Elector
) extends MongoCollection(
    db.getCollection("aws.buckets"),
    "aws.buckets",
    crawler,
    elector
) {}
