## Introduction

The [Zero to Docker] project tracks the Dockerfiles used to create official Netflix OSS containers
on [Docker Hub].

The [Edda container] provides an `edda.properties` template that drops the default MongoDB
configuration in favor of using DynamoDB for leader election and S3 for persistent storage of
the crawl results.  The launch technique for this container allows for the injection of AWS
keys through the template file for non-production testing purposes, although using on-instance
keys from an IAM role is more secure.

[Zero to Docker]: https://github.com/Netflix-Skunkworks/zerotodocker
[Docker Hub]: https://registry.hub.docker.com/repos/netflixoss/
[Edda container]: https://github.com/Netflix-Skunkworks/zerotodocker/wiki/Edda

```
# build command
docker build -t netflixoss/edda $DOCKERFILE_HOME

# interactive run command
docker run \
  --name edda \
  -p 8080:8080 \
  -v `pwd`/edda.properties:/tomcat/webapps/ROOT/WEB-INF/classes/edda.properties \
  netflixoss/edda:2.1

# detached run command
docker run -d \
  --name edda \
  -p 8080:8080 \
  -v `pwd`/edda.properties:/tomcat/webapps/ROOT/WEB-INF/classes/edda.properties \
  netflixoss/edda:2.1

# view port mappings
docker port edda

# connect interactive bash shell to a container
docker exec -it edda bash

# explore running containers
docker ps -a
docker start $CONTAINER_NAME_OR_ID
docker stop $CONTAINER_NAME_OR_ID
docker rm $CONTAINER_NAME_OR_ID

# explore container images
docker images
docker pull $IMAGE_NAME_OR_ID
docker rmi $IMAGE_NAME_OR_ID
docker rmi -f $IMAGE_NAME_OR_ID
```

The minimal `edda.properties` template is listed below.  The accessKey, secretKey and bucket
configuration options should be replaced with appropriate values prior to copying the file into
the container.

```
#-- change these properties ---------------------------------------------
edda.region=us-east-1
edda.aws.accessKey=yourAccessKey
edda.aws.secretKey=yourSecretKey
edda.s3current.bucket=yourBucket
#------------------------------------------------------------------------

edda.collection.aws.stacks.refresh=3600000
edda.collection.cache.refresh=30000
edda.collection.jitter.enabled=false
edda.collection.refresh=120000

edda.bean.argPattern=[^a-zA-Z0-9_]

edda.datastore.class=
edda.elector.class=com.netflix.edda.aws.DynamoDBElector
edda.elector.dynamodb.account=
edda.elector.dynamodb.leaderTimeout=60000
edda.elector.dynamodb.tableName=edda-leader
edda.elector.dynamodb.readCapacity=5
edda.elector.dynamodb.writeCapacity=1

edda.datastore.current.class=com.netflix.edda.aws.S3CurrentDatastore
edda.s3current.account=
edda.s3current.table=edda-s3current-collection-index-dev
edda.s3current.readCapacity=10
edda.s3current.writeCapacity=1
edda.s3current.locationPrefix=edda/s3current/dev
```

See [Configuration](./configuration.md) and [edda.properties] for details on additional
configuration options that can be set.

The Tomcat instance in the Edda container will run on port 8080 and this will be mapped to port
0.0.0.0:8080 on the host machine running the container.  Using this mapping, you can test Edda
with a curl command to one of the endpoints.  If you use curl for testing, you will want to
disable range queries by setting the globoff option.  See [REST API](./rest-api.md) for more
details on endpoints.

[edda.properties]: https://github.com/Netflix/edda/blob/master/src/main/resources/edda.properties

```
curl -g 'localhost:8080/api/v2/view/instances;_pp'
```

## Testing with Vagrant

The [Vagrant] project leverages [Virtual Box] and [published guest boxes] to automate the
provisioning of virtual machines on your local workstation.  You can use the following
`Vagrantfile` to stand up an environment for testing the Docker container.

[Vagrant]: https://www.vagrantup.com/
[Virtual Box]: https://www.virtualbox.org/
[published guest boxes]: https://vagrantcloud.com/boxes/search

```
# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
add-apt-repository ppa:webupd8team/java
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
apt-get update

JAVA_VER=8
echo oracle-java${JAVA_VER}-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
apt-get install -y --force-yes --no-install-recommends oracle-java${JAVA_VER}-installer oracle-java${JAVA_VER}-set-default 2>/dev/null

apt-get install -y git lxc-docker

mkdir git
pushd git
git clone https://github.com/Netflix/edda.git
git clone https://github.com/Netflix-Skunkworks/zerotodocker.git
popd
chown -R vagrant:vagrant git
SCRIPT

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provider "virtualbox" do |vb|
    vb.name = "nflxoss_trusty64"
    vb.memory = 4096
    vb.cpus = 2
  end
  config.vm.provision "shell", inline: $script
end
```

To use this `Vagrantfile`:

```
vagrant up
vagrant ssh
sudo docker ...
```

## Testing with Boot2Docker

The [Boot2Docker] project leverages [Virtual Box] and [Tiny Core Linux] to provide a thin
execution environment for Docker containers.  To allow access to the Edda container in the
VM from your terminal, you need to configure a [port forwarding rule] on the Virtual Box
guest.

[Boot2Docker]: http://boot2docker.io/
[Virtual Box]: https://www.virtualbox.org/
[Tiny Core Linux]: http://distro.ibiblio.org/tinycorelinux/
[port forwarding rule]: https://github.com/docker/docker/issues/4007

```
boot2docker init
VBoxManage modifyvm "boot2docker-vm" --natpf1 "tcp-port8080,tcp,,8080,,8080"
boot2docker up
$(boot2docker shellinit)

docker run \
  --name edda \
  -p 8080:8080 \
  -v `pwd`/edda.properties:/tomcat/webapps/ROOT/WEB-INF/classes/edda.properties \
  netflixoss/edda:2.1

curl -g 'localhost:8080/api/v2/view/instances;_pp'
```
