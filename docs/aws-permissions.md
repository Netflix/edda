## Example IAM Role Policy

```json
{
      "Statement": [{
          "Action": [
              "autoscaling:DescribeAutoScalingGroups",
              "autoscaling:DescribeLaunchConfigurations",
              "autoscaling:DescribePolicies",
              "cloudwatch:DescribeAlarms",
              "ec2:DescribeAddresses",
              "ec2:DescribeImages",
              "ec2:DescribeInstances",
              "ec2:DescribeReservedInstances",
              "ec2:DescribeSecurityGroups",
              "ec2:DescribeSnapshots",
              "ec2:DescribeTags",
              "ec2:DescribeVolumes",
              "elasticloadbalancing:DescribeInstanceHealth",
              "elasticloadbalancing:DescribeLoadBalancers",
              "iam:ListAccessKeys",
              "iam:ListGroupPolicies",
              "iam:ListGroups",
              "iam:ListGroupsForUser",
              "iam:ListRoles",
              "iam:ListUserPolicies",
              "iam:ListUsers",
              "iam:ListVirtualMFADevices",
              "s3:ListBucket",
              "s3:ListAllMyBuckets",
              "route53:ListHostedZones",
              "route53:ListResourceRecordSets",
              "sqs:GetQueueAttributes",
              "sqs:ListQueues",
              "rds:DescribeDBInstances"
          ],
          "Effect": "Allow",
          "Resource": "*"
      }]
  }
```
