= System Test

== Installing Dependencies

These steps should be required just once.

 - Install [Docker](https://docker.io)
 - Install [standalone kafka docker tool](https://github.com/DeviantArt/standalone-kafka), _in this directory_. It's already in `.gitignore`. If you already have it elsewhere or wish to share the installation, create a symlink so that we can find the helper scripts in `./system-test/standalone-kafka/kafka/bin/` relative to this repo's root.
  - Note that we rely on `auto.create.topics.enable = true` in the kafka broker config
```
$ git clone https://github.com/DeviantArt/standalone-kafka.git
$ cd standalone-kafka
$ docker build -t deviantart/standalone-kafka .
```
 - Install [FakeS3](https://github.com/jubos/fake-s3) (assumes you have ruby/gem installed)
```
$ [sudo] gem install fakes3
```
 - Install [kafka-python](https://github.com/dpkp/kafka-python)
```
$ [sudo] pip install kafka-python
```
 - Install [boto](https://github.com/boto/boto)
```
$ [sudo] pip install boto
```

== Setup Before Test Session

Since setup is somewhat expensive and complicated to automate in a highly portable fashion, we
make setup a somewhat manual step to perform before running tests.

==== 1. Ensure standalone docker image is running

Kafka must be accessible on `localhost:9092`.

Run it with:

```
$ docker run --name kafka -d --net=host -e HOSTNAME=localhost deviantart/standalone-kafka
```

The `name` param means you can use `$ docker kill kafka` when you're done.

On linux (where docker daemon is running directly on host kernel) you should be done.

For OS X or other setup where docker is running in a VM, you now need to forward localhost ports to the VM.

Assuming you used docker machine you can do this with:

```
$ docker-machine ssh default -f -N -L 9092:localhost:9092 -L 2181:localhost:2181
```

**Note:** you need a recent version of docker-machine for this to work. Known to work on 0.5.6.

The same should work with regular ssh for a non-docker machine VM.


== Running Tests

From the repo root dir, run:

```
$ python system_test/run.py
```