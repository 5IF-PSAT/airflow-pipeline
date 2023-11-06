rs.initiate(
  {
    _id: "config_rs",
    configsvr: true,
    members: [
      { _id : 0, host : "configsvr1:27017" },
      { _id : 1, host : "configsvr2:27017" },
      { _id : 2, host : "configsvr3:27017" }
    ]
  }
)

rs.initiate(
  {
    _id: "shard1_rs",
    members: [
      { _id : 0, host : "shardsvr1_1:27017" },
      { _id : 1, host : "shardsvr1_2:27017" },
      { _id : 2, host : "shardsvr1_3:27017" }
    ]
  }
)

rs.initiate(
  {
    _id: "shard2_rs",
    members: [
      { _id : 0, host : "shardsvr2_1:27017" },
      { _id : 1, host : "shardsvr2_2:27017" },
      { _id : 2, host : "shardsvr2_3:27017" }
    ]
  }
)

sh.addShard("shard1_rs/shardsvr1_1:27017,shardsvr1_2:27017,shardsvr1_3:27017")
sh.addShard("shard2_rs/shardsvr2_1:27017,shardsvr2_2:27017,shardsvr2_3:27017")

sh.shardCollection("deng.bus-delay", { "year": 1, "month": 1, "day": 1, "hour": 1 } )
