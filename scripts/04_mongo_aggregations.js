// STEP 4 - Track A element: analytics done natively inside MongoDB.
// Run:  mongosh "mongodb://localhost:27017/fraudlens" --file 04_mongo_aggregations.js
// Needs MongoDB 5.0+ for $setWindowFields / $count-window. Each query prints its runtime.

const d = db.getSiblingDB("fraudlens");   // change here if you changed MONGO_DB

function run(title, coll, pipeline) {
  print("\n==================================================");
  print(title);
  print("==================================================");
  const t0 = Date.now();
  try {
    const res = d.getCollection(coll).aggregate(pipeline, { allowDiskUse: true }).toArray();
    print("-> runtime: " + (Date.now() - t0) + " ms | rows returned: " + res.length);
    res.forEach(r => printjson(r));
  } catch (e) {
    print("!! query failed (MongoDB version too old for this operator?): " + e.message);
  }
}

print("Sample nested document:");
printjson(d.transactions.findOne({ is_fraud: 1 }));
print("Indexes:");
d.transactions.getIndexes().forEach(i => print("  " + i.name + " -> " + JSON.stringify(i.key)));

// ---------------- Q1: fraud rate by transaction type
run("Q1  Fraud rate by transaction type", "transactions", [
  { $group: { _id: "$type", tx: { $sum: 1 }, fraud: { $sum: "$is_fraud" }, avg_amount: { $avg: "$amount" } } },
  { $addFields: {
      fraud_rate_pct: { $round: [{ $multiply: [{ $divide: ["$fraud", "$tx"] }, 100] }, 3] },
      avg_amount: { $round: ["$avg_amount", 2] } } },
  { $sort: { fraud_rate_pct: -1 } }
]);

// ---------------- Q2: fraud by merchant category
run("Q2  Fraud by merchant category", "transactions", [
  { $group: { _id: "$merchant_category", tx: { $sum: 1 }, fraud: { $sum: "$is_fraud" },
              fraud_amount: { $sum: { $cond: [{ $eq: ["$is_fraud", 1] }, "$amount", 0] } } } },
  { $addFields: { fraud_rate_pct: { $round: [{ $multiply: [{ $divide: ["$fraud", "$tx"] }, 100] }, 3] } } },
  { $sort: { fraud_rate_pct: -1 } }
]);

// ---------------- Q3: dynamic bucket categorisation of amounts
run("Q3  $bucket - amount bands vs fraud", "transactions", [
  { $bucket: {
      groupBy: "$amount",
      boundaries: [0, 1000, 10000, 50000, 100000, 200000, 500000, 1000000, 1000000000],
      default: "other",
      output: { tx: { $sum: 1 }, fraud: { $sum: "$is_fraud" }, avg_amount: { $avg: "$amount" } } } },
  { $addFields: { fraud_rate_pct: { $round: [{ $multiply: [{ $divide: ["$fraud", "$tx"] }, 100] }, 3] },
                  avg_amount: { $round: ["$avg_amount", 2] } } }
]);

// ---------------- Q4: rolling analytics with $setWindowFields (24-hour window per device)
const win = { range: [-24, 0] };      // 'step' = 1 hour, so this is the previous 24 hours
run("Q4a $setWindowFields - rolling 24h device activity: fraud vs normal", "transactions", [
  { $setWindowFields: { partitionBy: "$device.id", sortBy: { step: 1 },
      output: { tx_24h: { $count: {}, window: win }, avg_amt_24h: { $avg: "$amount", window: win } } } },
  { $group: { _id: "$is_fraud", avg_tx_24h: { $avg: "$tx_24h" }, max_tx_24h: { $max: "$tx_24h" },
              avg_amt_24h: { $avg: "$avg_amt_24h" } } }
]);
run("Q4b Devices with a burst of >= 15 transactions in 24h (top 5 rows)", "transactions", [
  { $setWindowFields: { partitionBy: "$device.id", sortBy: { step: 1 },
      output: { tx_24h: { $count: {}, window: win } } } },
  { $match: { tx_24h: { $gte: 15 } } },
  { $project: { _id: 0, tx_id: 1, "device.id": 1, step: 1, amount: 1, tx_24h: 1, is_fraud: 1 } },
  { $sort: { tx_24h: -1 } }, { $limit: 5 }
]);

// ---------------- Q5: joins with $lookup (transactions -> devices)
run("Q5a Top 5 riskiest devices (from the 'devices' collection built with $merge)", "devices", [
  { $match: { tx_count: { $gte: 5 } } },
  { $sort: { fraud_rate: -1, tx_count: -1 } }, { $limit: 5 }
]);
run("Q5b $lookup - enrich fraud TRANSFERs with device-level history", "transactions", [
  { $match: { is_fraud: 1, type: "TRANSFER" } }, { $limit: 2000 },
  { $lookup: { from: "devices", localField: "device.id", foreignField: "_id", as: "dev" } },
  { $unwind: "$dev" },
  { $group: { _id: null, avg_device_tx_count: { $avg: "$dev.tx_count" },
              avg_device_fraud_rate: { $avg: "$dev.fraud_rate" }, n: { $sum: 1 } } }
]);

// ---------------- Q6: graph traversal with $graphLookup (money-mule chains)
run("Q6  $graphLookup - follow fraud money chains (TRANSFER -> CASH_OUT -> ...) up to 3 hops", "transactions", [
  { $match: { is_fraud: 1, type: "TRANSFER" } }, { $limit: 5 },
  { $graphLookup: { from: "transactions", startWith: "$dest.account",
      connectFromField: "dest.account", connectToField: "origin.account", as: "chain",
      maxDepth: 3, depthField: "hop", restrictSearchWithMatch: { is_fraud: 1 } } },
  { $project: { _id: 0, tx_id: 1, amount: 1, "origin.account": 1, "dest.account": 1,
                chain_length: { $size: "$chain" }, chain_tx_ids: "$chain.tx_id", chain_types: "$chain.type" } }
]);

// ---------------- Q7: geospatial query (2dsphere index)
run("Q7  Geo - transactions within 100 km of Mumbai (fraud vs normal)", "transactions", [
  { $match: { location: { $geoWithin: { $centerSphere: [[72.8777, 19.0760], 100 / 6378.1] } } } },
  { $group: { _id: "$is_fraud", tx: { $sum: 1 }, avg_amount: { $avg: "$amount" } } }
]);
run("Q7b Foreign-country transactions (tag array) - fraud rate", "transactions", [
  { $addFields: { is_foreign: { $in: ["foreign", "$tags"] } } },
  { $group: { _id: "$is_foreign", tx: { $sum: 1 }, fraud: { $sum: "$is_fraud" } } },
  { $addFields: { fraud_rate_pct: { $round: [{ $multiply: [{ $divide: ["$fraud", "$tx"] }, 100] }, 3] } } }
]);

// ---------------- Q8: daily trend
run("Q8  Daily fraud trend (first 10 days)", "transactions", [
  { $group: { _id: { $dateToString: { format: "%Y-%m-%d", date: "$ts" } }, tx: { $sum: 1 }, fraud: { $sum: "$is_fraud" } } },
  { $sort: { _id: 1 } }, { $limit: 10 }
]);

// ---------------- Q9: indexing effect - explain() with and without index
print("\n==================================================");
print("Q9  Indexing: index scan vs collection scan (explain executionStats)");
print("==================================================");
const acct = d.transactions.findOne({ is_fraud: 1 }).origin.account;
function plan(label, cursor) {
  const e = cursor.explain("executionStats");
  print(label + " -> time: " + e.executionStats.executionTimeMillis + " ms | docsExamined: " +
        e.executionStats.totalDocsExamined + " | keysExamined: " + e.executionStats.totalKeysExamined);
  print("   plan: " + JSON.stringify(e.queryPlanner.winningPlan).substring(0, 160));
}
plan("WITH index on origin.account", d.transactions.find({ "origin.account": acct }));
plan("NO index (forced collection scan)", d.transactions.find({ "origin.account": acct }).hint({ $natural: 1 }));
