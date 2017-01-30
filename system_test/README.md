# System Test

## Installing Dependencies

These steps should be required just once.

 - Install [Docker](https://docker.io)
 - Install [standalone kafka docker tool](https://github.com/DeviantArt/standalone-kafka), _in the `system_test` directory_. It's already in `.gitignore`. If you already have it elsewhere or wish to share the installation, create a symlink so that we can find the helper scripts in `./system-test/standalone-kafka/kafka/bin/` relative to this repo's root.
  - Note that we rely on `auto.create.topics.enable = true` in the kafka broker config
```sh
$ git clone https://github.com/DeviantArt/standalone-kafka.git
$ cd standalone-kafka
$ docker build -t deviantart/standalone-kafka .
```
 - Install [FakeS3](https://github.com/jubos/fake-s3) (assumes you have ruby/gem installed)
```sh
$ [sudo] gem install fakes3
```
 - Install [kafka-python](https://github.com/dpkp/kafka-python)
```sh
$ [sudo] pip install kafka-python
```
 - Install [boto](https://github.com/boto/boto)
```sh
$ [sudo] pip install boto
```

## Setup Before Test Session

Since setup is somewhat expensive and complicated to automate in a highly portable fashion, we
make setup a somewhat manual step to perform before running tests.

### Start standalone-kafka Docker image

Kafka must be accessible on `localhost:9092`.

Run it with:

```sh
$ docker run -d -p 2181:2181 -p 9092:9092 --name kafka deviantart/standalone-kafka
```

The `name` param means you can use `$ docker kill kafka` when you're done.

### Map ports (old versions of Docker)

On newer versions of Docker (where Docker daemon is running directly on host kernel) you should be done.

Older versions of Docker, where docker is running inside of a virtual machine, you now need to forward localhost ports to the virtual machine.

Assuming you're using [docker-machine](https://docs.docker.com/machine/overview/) to manage the virtual machine, you can map the ports with:

```sh
$ docker-machine ssh default -f -N -L 9092:localhost:9092 -L 2181:localhost:2181
```

**Note:** you need a recent version of docker-machine for this to work. Known to work on 0.5.6.

The same should work with regular ssh for a non-docker machine VM.


## Running Tests

From the repo root dir, run:

```sh
$ python system_test/run.py
```
