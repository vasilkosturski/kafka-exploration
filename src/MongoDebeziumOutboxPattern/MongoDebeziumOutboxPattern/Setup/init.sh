docker-compose -f docker-compose-mongodb.yaml up -d

# Check the status of MongoDB and wait until it's ready
until docker run --network host --rm mongo:6.0 mongosh --host localhost --eval "db.adminCommand('ping')"
do
    echo "Waiting for MongoDB..."
    sleep 5s
done

# Initiate the replica set
echo "Initiating the MongoDB replica set"
docker run --network host --rm mongo:6.0 mongosh --host localhost --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})'
echo "MongoDB replica set initiated"

# Wait for the replica set to be fully initialized
until docker run --network host --rm mongo:6.0 mongosh --host localhost --eval 'rs.status().myState == 1'
do
    echo "Waiting for MongoDB replica set to fully initialize..."
    sleep 3s
done
echo "MongoDB replica set is fully initialized."

echo "Creating 'testdb' database and 'outbox' collection..."
docker run --network host --rm mongo:6.0 mongosh --host localhost --eval '
    db = db.getSiblingDB("testdb");
    db.createCollection("outbox");
'
echo "Database 'testdb' and collection 'outbox' created."

sleep 10s

# Start MongoDB connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mongodb.json

echo "Debezium connector created."
