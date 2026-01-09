var now = new Date().toISOString();

var adminDb = db.getSiblingDB("admin");
var list = adminDb.runCommand({ listDatabases: 1 });
if (!list || list.ok !== 1) {
  throw new Error("listDatabases failed");
}

var dataBytes = 0;
var indexBytes = 0;
var physicalBytes = 0;

for (var i = 0; i < list.databases.length; i++) {
  var dbName = list.databases[i].name;
  if (dbName === "admin" || dbName === "local" || dbName === "config") {
    continue;
  }

  var stats = db.getSiblingDB(dbName).stats();
  if (!stats || stats.ok !== 1) {
    continue;
  }

  dataBytes += stats.dataSize || 0;
  indexBytes += stats.indexSize || 0;

  var storageSize = stats.storageSize || 0;
  var indexSize = stats.indexSize || 0;
  physicalBytes += storageSize + indexSize;
}

var logicalTotal = dataBytes + indexBytes;
var serverStatus = adminDb.serverStatus();
var mongoVersion = serverStatus && serverStatus.version ? serverStatus.version : "";

print([
  now,
  dataBytes,
  indexBytes,
  logicalTotal,
  physicalBytes,
  mongoVersion
].join("\t"));
