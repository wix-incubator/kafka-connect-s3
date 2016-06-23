import unittest
import subprocess
import os
import shutil
import time
from datetime import date
import sys
import socket
from kafka import KafkaProducer
import json
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import zlib
import xml.etree.ElementTree as ET

this_dir = os.path.dirname(__file__)

fakes3_data_path = os.path.join(this_dir, 'data')
fixture_path = os.path.join(this_dir, 'fixture')
# as configured in system-test-s3-sink.properties. might not be super portable
# but simplest option for now without needing to dynamically write that config
# for each run...
connect_data_path = '/tmp/connect-system-test'

g_fakes3_proc = None
g_s3connect_proc = None

def modulo_partitioner(key, all_partitions, available_partitions):
    if key is None:
        key = 0
    idx = int(key) % len(all_partitions)
    return all_partitions[idx]

g_producer = KafkaProducer(bootstrap_servers="localhost:9092",
                          partitioner=modulo_partitioner,
                          metadata_max_age_ms=1000);

g_s3_conn = S3Connection('foo', 'bar', is_secure=False, port=9090, host='localhost',
                        calling_format=OrdinaryCallingFormat())


def connect_version():
    # quick hack to get version from pom.
    tree = ET.parse(os.path.join(this_dir, '..', 'pom.xml'))
    root = tree.getroot()
    version = root.find('{http://maven.apache.org/POM/4.0.0}version').text
    return version

# requires proc to be Popened with stdout=subprocess.PIPE,stderr=subprocess.STDOUT
def dumpServerStdIO(proc, msg, until=None, until_fail=None, timeout=None, trim_indented=False, post_fail_lines=20):
    sys.stdout.write(msg + os.linesep)

    if not proc:
        return (False, None)

    start = time.time()
    # After we see fail, add another few lines to see the full error
    post_fail_lines_remaining = post_fail_lines
    fail_line = None
    while True:
        try:
            line = proc.stdout.readline()
            if not line:
                break

            if fail_line is not None:
                if post_fail_lines_remaining <= 0:
                    return (False, fail_line)
                else:
                    sys.stderr.write("    STDIO> " + line)
                    post_fail_lines_remaining -= 1
                    continue

            if not trim_indented or not line.startswith((' ', '\t')):
                sys.stderr.write("    STDIO> " + line)

            if until_fail and line.find(until_fail) >= 0:
                fail_line = line
                continue
            if until and line.find(until) >= 0:
                return (True, line)

            if timeout is not None and (time.time() - start) > timeout:
                return (False, "Timedout after {} second".format(time.time() - start))

        except (KeyboardInterrupt, SystemExit):
            tearDownModule()
            sys.exit(1)

    return (True, None)

def setUpModule():
    global g_fakes3_proc, g_s3_conn

    # Clean up data from previous runs
    if os.path.isdir(fakes3_data_path):
        shutil.rmtree(fakes3_data_path)

    if os.path.isdir('/tmp/connect-system-test'):
        shutil.rmtree('/tmp/connect-system-test')

    # Recreate the dirs!
    os.mkdir(fakes3_data_path)
    os.mkdir(connect_data_path)

    # Clear our topic from Kafka
    try:
        subprocess.check_output([os.path.join(this_dir, 'standalone-kafka/kafka/bin/kafka-topics.sh'),
                               '--zookeeper', 'localhost:2181', '--delete', '--topic', 'system-test']);
    except subprocess.CalledProcessError as e:
        # if the complaint is that the topic doesn't exist then ignore it, otherwise fail loudly
        if e.output.find("Topic system-test does not exist on ZK path") < 0:
            raise e

    # Recreate fresh
    output = subprocess.check_output([os.path.join(this_dir, 'standalone-kafka/kafka/bin/kafka-topics.sh'),
                                    '--zookeeper', 'localhost:2181', '--create', '--topic', 'system-test',
                                    '--partitions', '1', '--replication-factor', '1']);
    if output != "Created topic \"system-test\".\n":
        raise RuntimeError("Failed to create test topic:\n{}".format(output))

    # Run fakeS3
    print "Starting FakeS3..."
    g_fakes3_proc = subprocess.Popen(['fakes3', '-p', '9090', '-r', fakes3_data_path],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT);

    try:
        print "While we wait, let's do very basic check that kafka is up"
        sock = socket.create_connection(('localhost', 9092), 1)
        # Connected without throwing timeout exception so just close again
        sock.close();
        print "Great, Kafka seems to be there."

        dumpServerStdIO(g_fakes3_proc, "Just waiting for FakeS3 to be ready...", "WEBrick::HTTPServer#start");

        # ensure bucket is created
        g_s3_conn.create_bucket('connect-system-test')

        print "SETUP DONE"

    except:
        tearDownModule()
        raise

def tearDownModule():
    global g_fakes3_proc
    if g_fakes3_proc is not None:
        print "Terminating FakeS3"
        g_fakes3_proc.kill()
        g_fakes3_proc.wait()

    print "TEARDOWN DONE"

def runS3ConnectStandalone(worker_props ='system-test-worker.properties', sink_props ='system-test-s3-sink.properties', debug=False):
    global g_s3connect_proc
    version = connect_version()
    env = {
        'CLASSPATH': os.path.join(this_dir, '../target/kafka-connect-s3-{}.jar'.format(version))
    }
    if debug:
        env['KAFKA_JMX_OPTS'] = '-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=y'
    cmd = [os.path.join(this_dir, 'standalone-kafka/kafka/bin/connect-standalone.sh'),
           os.path.join(this_dir, worker_props),
           os.path.join(this_dir, sink_props)]

    g_s3connect_proc = subprocess.Popen(cmd,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       env=env)

    dumpServerStdIO(g_s3connect_proc,
                   "Wait for S3 connect initialisation...",
                   "finished initialization and start",
                   trim_indented=True)

    return g_s3connect_proc

class TestConnectS3(unittest.TestCase):
    def tearDown(self):
        if g_s3connect_proc is not None:
            print "Terminating Kafka Connect"
            g_s3connect_proc.kill()
            g_s3connect_proc.wait()

    '''
    These tests are highly non-deterministic, but they pass almost always on my local setup.

    Controlling things like time, exactly when connect flushes etc are not really possible.

    Making validation here so smart that it can correctly identify any valid output of the system
    without false positives is a strictly harder programming problem than the system under test.

    So this serves as a manually-run set of smoke tests that sanity check the integration logic of
    the implementation, and automate a many-step ad-hoc testing environment.
    '''
    def test_basic_consuming(self):
        global g_producer
        topic = "system-test"

        s3connect = runS3ConnectStandalone()

        # messages produced asynchronously - synchronous producing makes it likely
        # they will be split into different flushes in connect
        expected_data = ''
        for i in range(0, 100):
            record = b'{{"foo": "bar", "counter":{}}}'.format(i)
            g_producer.send(topic, record)
            expected_data += record + '\n'

        ok, line = dumpServerStdIO(s3connect, "Wait for connect to process and commit",
                                  until="Successfully uploaded chunk for system-test-0",
                                  until_fail="ERROR",
                                  timeout=15, trim_indented=True)

        self.assertTrue(ok, msg="Didn't get success message but did get: {}".format(line))

        today = date.today()

        pfx = 'systest/{}/'.format(today.isoformat())

        # Fetch the files written and assert they are as expected
        self.assert_s3_file_contents('systest/last_chunk_index.system-test-00000.txt',
                                     pfx+'system-test-00000-000000000000.index.json')

        self.assert_s3_file_contents(pfx+'system-test-00000-000000000000.index.json',
                                    '{"chunks":[{"byte_length_uncompressed":2890,"num_records":100,"byte_length":275,"byte_offset":0,"first_record_offset":0}]}')


        self.assert_s3_file_contents(pfx+'system-test-00000-000000000000.gz', expected_data, gzipped=True)

        # Now stop the connect process and restart it and ensure it correctly resumes from where we left
        print "Restarting Kafka Connect"
        s3connect.kill()
        s3connect.wait()

        # produce 100 more entries
        expected_data = ''
        for i in range(100, 200):
            record = b'{{"foo": "bar", "counter":{}}}'.format(i)
            g_producer.send(topic, record)
            expected_data += record + '\n'

        # restart connect
        s3connect = runS3ConnectStandalone()

        ok, line = dumpServerStdIO(s3connect, "Wait for connect to process and commit",
                                  until="Successfully uploaded chunk for system-test-0",
                                  until_fail="ERROR",
                                  timeout=15, trim_indented=True)

        self.assertTrue(ok, msg="Didn't get success message but did get: {}".format(line))

        today = date.today()

        pfx = 'systest/{}/'.format(today.isoformat())

        # Fetch the files written and assert they are as expected
        self.assert_s3_file_contents('systest/last_chunk_index.system-test-00000.txt',
                                     pfx+'system-test-00000-000000000100.index.json')

        self.assert_s3_file_contents(pfx+'system-test-00000-000000000100.index.json',
                                    '{"chunks":[{"byte_length_uncompressed":3000,"num_records":100,"byte_length":272,"byte_offset":0,"first_record_offset":100}]}')


        self.assert_s3_file_contents(pfx+'system-test-00000-000000000100.gz', expected_data, gzipped=True)

        # now we test reconfiguring the topic to have more partitions...
        print "Reconfiguring topic..."
        output = subprocess.check_output([os.path.join(this_dir, 'standalone-kafka/kafka/bin/kafka-topics.sh'),
                                    '--zookeeper', 'localhost:2181', '--alter', '--topic', 'system-test',
                                    '--partitions', '3']);

        if not output.endswith("Adding partitions succeeded!\n"):
            raise RuntimeError("Failed to reconfigure test topic:\n{}".format(output))

        # wait for out producer to catch up with the reconfiguration otherwise we'll keep producing only
        # to the single partition
        while len(g_producer.partitions_for('system-test')) < 3:
            print "Waiting for new partitions to show up in producer"
            time.sleep(0.5)

        # produce some more, this time with keys so we know where they will end up
        expected_partitions = ['','','']
        for i in range(200, 300):
            record = b'{{"foo": "bar", "counter":{}}}'.format(i)
            g_producer.send(topic, key=bytes(i), value=record)
            expected_partitions[i % 3] += record + '\n'

        # wait for all three partitions to commit (not we don't match partition number as)
        # we can't assume what order they will appear in.
        ok, line = dumpServerStdIO(s3connect, "Wait for connect to process and commit 1/3",
                                  until="Successfully uploaded chunk for system-test-",
                                  until_fail="ERROR",
                                  timeout=15, trim_indented=True)
        self.assertTrue(ok, msg="Didn't get success message but did get: {}".format(line))

        ok, line = dumpServerStdIO(s3connect, "Wait for connect to process and commit 2/3",
                                  until="Successfully uploaded chunk for system-test-",
                                  until_fail="ERROR",
                                  timeout=15, trim_indented=True)
        self.assertTrue(ok, msg="Didn't get success message but did get: {}".format(line))

        ok, line = dumpServerStdIO(s3connect, "Wait for connect to process and commit 3/3",
                                  until="Successfully uploaded chunk for system-test-",
                                  until_fail="ERROR",
                                  timeout=15, trim_indented=True)
        self.assertTrue(ok, msg="Didn't get success message but did get: {}".format(line))

        # partition 0
        self.assert_s3_file_contents('systest/last_chunk_index.system-test-00000.txt',
                                     pfx+'system-test-00000-000000000200.index.json')

        self.assert_s3_file_contents(pfx+'system-test-00000-000000000200.index.json',
                                    '{"chunks":[{"byte_length_uncompressed":990,"num_records":33,"byte_length":137,"byte_offset":0,"first_record_offset":200}]}')


        self.assert_s3_file_contents(pfx+'system-test-00000-000000000200.gz', expected_partitions[0], gzipped=True)

        # partition 1 (new partition will start from offset 0)
        self.assert_s3_file_contents('systest/last_chunk_index.system-test-00001.txt',
                                     pfx+'system-test-00001-000000000000.index.json')

        self.assert_s3_file_contents(pfx+'system-test-00001-000000000000.index.json',
                                    '{"chunks":[{"byte_length_uncompressed":990,"num_records":33,"byte_length":137,"byte_offset":0,"first_record_offset":0}]}')


        self.assert_s3_file_contents(pfx+'system-test-00001-000000000000.gz', expected_partitions[1], gzipped=True)

        # partition 2 (new partition will start from offset 0)
        self.assert_s3_file_contents('systest/last_chunk_index.system-test-00002.txt',
                                     pfx+'system-test-00002-000000000000.index.json')

        self.assert_s3_file_contents(pfx+'system-test-00002-000000000000.index.json',
                                    '{"chunks":[{"byte_length_uncompressed":1020,"num_records":34,"byte_length":139,"byte_offset":0,"first_record_offset":0}]}')


        self.assert_s3_file_contents(pfx+'system-test-00002-000000000000.gz', expected_partitions[2], gzipped=True)

    def test_binary_format(self):
        global g_producer
        topic = "binary-system-test"

        # delete the topic before we start so we don't get extra messages
        sys.stderr.write(subprocess.check_output([os.path.join(this_dir, 'standalone-kafka/kafka/bin/kafka-topics.sh'),
                         '--zookeeper', 'localhost:2181', '--delete', '--topic', topic]))

        debug=False
        s3connect = runS3ConnectStandalone(
            debug=debug,
            worker_props ='system-test-binary-worker.properties',
            sink_props ='system-test-s3-binary-sink.properties')

        # messages produced asynchronously - synchronous producing makes it likely
        # they will be split into different flushes in connect
        expected_data = ''
        for i in range(0, 100):
            record = b'{}\n'.format(i)
            g_producer.send(topic, record)
            expected_data += record

        g_producer.flush()

        ok, line = dumpServerStdIO(s3connect, "Wait for binary sink to process and commit",
                                   until="Successfully uploaded chunk for binary-system-test-0",
                                   until_fail="ERROR",
                                   timeout=(1000000 if debug else 15), trim_indented=True)

        self.assertTrue(ok, msg="Didn't get success message but did get: {}".format(line))

        # run the reader to dump what we just wrote
        version = connect_version()
        env = {
            'CLASSPATH': os.path.join(this_dir, '../target/kafka-connect-s3-{}.jar'.format(version))
        }
        cmd = [os.path.join(this_dir, 'standalone-kafka/kafka/bin/kafka-run-class.sh'),
               "com.deviantart.kafka_connect_s3.S3FilesReader",
               os.path.join(this_dir, 'system-test-s3-binary-sink.properties')]

        self.assertEqual(expected_data, subprocess.check_output(cmd, env=env))


    def assert_s3_file_contents(self, key, content, gzipped=False, encoding="utf-8"):
        global g_s3_conn
        bucket = g_s3_conn.get_bucket('connect-system-test')
        file = bucket.get_key(key)
        actual_contents = file.get_contents_as_string()
        if gzipped:
            # Hacks, http://stackoverflow.com/questions/2695152/in-python-how-do-i-decode-gzip-encoding
            actual_contents = zlib.decompress(actual_contents, 16+zlib.MAX_WBITS)

        self.assertEqual(content, actual_contents.decode(encoding))


if __name__ == '__main__':
    unittest.main()
