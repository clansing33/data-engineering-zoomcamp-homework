### Question 1: Redpanda version

To get into the container
```bash
docker exec -it redpanda-1 sh
```

```bash
rpk version
```

#### Answer
```
$ rpk version
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```


### Question 2. Creating a topic

#### Answer
```
$ rpk topic create green-trips              
TOPIC        STATUS
green-trips  OK
```


### Question 3. Connecting to the Kafka server

#### Answer
True


### Question 4: Sending the Trip Data

Send messages to redpanda(kafka)
```bash
cd 06-streaming/pyflink
```

load_taxi_data.py <- Run python file


Use Flink SQL client (inside container)
```bash
docker ps | grep flink

docker exec -it flink-jobmanager /bin/sh

/opt/flink/bin/sql-client.sh

```

#### Answer
took 40.07 seconds
