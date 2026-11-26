const namespace = "nyc_traffic.violations_500k";

print("Setting up hash sharding for: " + namespace);

const result = sh.shardCollection(
  namespace,
  { vehicle_hash: "hashed" }
);

printjson(result);