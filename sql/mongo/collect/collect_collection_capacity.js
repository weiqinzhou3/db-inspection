var now = new Date().toISOString();

var DOC_THRESHOLD = 5000000;
var SIZE_THRESHOLD = 50 * 1024 * 1024 * 1024;

var adminDb = db.getSiblingDB("admin");
var list = adminDb.runCommand({ listDatabases: 1 });
if (!list || list.ok !== 1) {
  throw new Error("listDatabases failed");
}

for (var i = 0; i < list.databases.length; i++) {
  var dbName = list.databases[i].name;
  if (dbName === "admin" || dbName === "local" || dbName === "config") {
    continue;
  }

  var targetDb = db.getSiblingDB(dbName);
  var collections = targetDb.getCollectionNames();

  for (var j = 0; j < collections.length; j++) {
    var collName = collections[j];
    if (collName.indexOf("system.") === 0) {
      continue;
    }

    var stats;
    try {
      stats = targetDb.getCollection(collName).stats();
    } catch (e) {
      continue;
    }

    if (!stats || stats.ok !== 1) {
      continue;
    }

    var docCount = stats.count || 0;
    var dataBytes = stats.size || 0;
    var indexBytes = stats.totalIndexSize || 0;
    var logicalTotal = dataBytes + indexBytes;
    var physicalTotal = (stats.storageSize || 0) + indexBytes;

    if (docCount > DOC_THRESHOLD || logicalTotal > SIZE_THRESHOLD) {
      print([
        now,
        dbName,
        collName,
        docCount,
        dataBytes,
        indexBytes,
        logicalTotal,
        physicalTotal
      ].join("\t"));
    }
  }
}
