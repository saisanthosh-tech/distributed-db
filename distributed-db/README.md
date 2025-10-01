How to Run the Sharded, Resilient Distributed Database
This is the final version of the database. It is fault-tolerant (survives leader crashes) and scalable (data is partitioned across multiple shards).

It consists of two main components:

Database Nodes (database.go): These run in groups (shards) and store the actual data.

Router (router.go): A single entry point that directs client requests to the correct shard.

1. Setup
You should have two files in your project directory:

database.go (the updated sharded version)

router.go (the new router program)

You will need seven terminal windows to run the full system: one for the router, three for Shard 0, and three for Shard 1.

2. Running the System
Run these commands in your seven terminals.

Terminal 1: Start the Router

go run router.go

This will start the main entry point on port :7070.

Terminals 2, 3, 4: Start Shard 0

Terminal 2 (Shard 0 Leader):

go run database.go --addr=":8080" --shard=0

Terminal 3 (Shard 0 Follower):

go run database.go --addr=":8081" --shard=0

Terminal 4 (Shard 0 Follower):

go run database.go --addr=":8082" --shard=0

Terminals 5, 6, 7: Start Shard 1

Terminal 5 (Shard 1 Leader):

go run database.go --addr=":9090" --shard=1

Terminal 6 (Shard 1 Follower):

go run database.go --addr=":9091" --shard=1

Terminal 7 (Shard 1 Follower):
go run database.go --addr=":9092" --shard=1

Your entire sharded database is now online!

3. Testing Sharding
Open an eighth terminal to act as your client. All commands now go to the router on port :7070.

Step 1: Write two different keys

The router will automatically send them to different shards based on their hash.

# This key will be routed to one of the shards (e.g., Shard 0)
curl -X PUT -d "value-for-apple" http://localhost:7070/keys/apple

# This key will likely be routed to the other shard (e.g., Shard 1)
curl -X PUT -d "value-for-zebra" http://localhost:7070/keys/zebra

Look at the logs in your router terminal! You'll see it printing which shard each key was sent to.

Step 2: Read the data back

You can read both keys from the router, and it will fetch them from the correct shard.

curl http://localhost:7070/keys/apple
# Should return: value-for-apple

curl http://localhost:7070/keys/zebra
# Should return: value-for-zebra

Step 3 (Optional): Test Failover Within a Shard

You can repeat the leader crash experiment. For example, shut down the leader of Shard 0 (:8080).

The other Shard 0 nodes (:8081, :8082) will elect a new leader.

Crucially, Shard 1 will be completely unaffected.

The simple router we built won't automatically discover the new leader (a production router would), but you have proven the self-healing capability of the shard itself.

You have now built a complete, scalable, and resilient distributed database from the ground up. This covers the entire lifecycle of a distributed system, from a single node to a globally scalable architecture.