/*
 * Requires SSH tunnel to DocumentDB to be set on the PORT constant, as well as DOC_DB_USER/PASS_PERF 
 * environment variables to be set appropriately for DATABASE constant.
 * Args: --collection --numDocs --type --shouldClear
 * collection: The name of the collection to insert documents to.
 * numDocs: The number of documents that should be added.
 * type: The type of document to insert, must be one of main, fields, array, random, demo.
 * shouldClear: Optional, if true will clear given collection before inserting documents.
 */

const USER = process.env.DOC_DB_USER_NAME;
const PASSWORD = process.env.DOC_DB_PASSWORD;
const PORT = process.env.DOC_DB_LOCAL_PORT;
const DATABASE = "performance";

const MAIN = "main";
const FIELDS = "fields";
const ARRAY = "array";
const RANDOM = "random";
const DEMO = "demo";

const LOT_SIZE = 100000;

const MongoClient = require('mongodb').MongoClient,
    f = require('util').format,
    fs = require('fs');

const argv = require('yargs').argv;
const faker = require('faker');

const url = `mongodb://${USER}:${PASSWORD}@localhost:${PORT}/${DATABASE}?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&tlsAllowInvalidHostNames=true`;

const COLLECTION = argv.collection;
const NUM_DOCS = parseInt(argv.numDocs);
const TYPE = argv.type;
const SHOULD_CLEAR = argv.shouldClear === "true";

if (argv.h) {
    console.log("Parameters: \n" +
        "--collection=[name of collection]\n" +
        "--numDocs=[number of documents to add]\n" +
        "--type=[type of document, one of main, fields, array, random, or demo]\n" +
        "--shouldClear=[If true, will delete existing collection before replacing]");
    process.exit();
}

if (typeof COLLECTION === "undefined") {
    throw Error("Must provide collection name in paramenters (--collection).");
}
if (typeof NUM_DOCS === "undefined" || isNaN(NUM_DOCS)) {
    throw Error("Must provide number of documents in paramenters (--numDocs).");
}
if (typeof TYPE === "undefined") {
    throw Error("Must provide type of document in paramenters (--type).");
}

console.log(`Begin creation of performance collections ${new Date().toJSON()}`);

MongoClient.connect( // docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html
    url,
    {
        useNewUrlParser: true
    },
    function (err, client) {
        if (err) throw err;
        var db = client.db(DATABASE);

        // Delete existing collection
        if (SHOULD_CLEAR) {
            dropCollection(db);
        }
        insertDocuments(db, client);
    }
);


async function dropCollection(db, collection) {
    var collections = await db.listCollections().toArray();
    collections = collections.map(collection => {
        return collection.name;
    });
    if (collections.includes(COLLECTION)) {
        // Delete existing collection
        await db.collection(COLLECTION).drop()
            .then(res => {
                console.log("Successfully removed collection.");
            })
            .catch(err => {
                console.log("Could not clear collection");
            });
    } else {
        console.log("Collection not found, no removal necessary.")
    }
}

/*
 * Adds documents to the database. Uses smaller lots of 100000 to prevent out of memory errors.
 */
async function insertDocuments(db, client) {
    var lots = Math.floor(NUM_DOCS / LOT_SIZE);
    for (var i = 0; i < lots; i++) {
        await db.collection(COLLECTION).insertMany(createDocuments(LOT_SIZE))
        .catch(err => {
            console.log("Failed to add documents to collection.");
            client.close();
            throw (err);
        });
    }
    var remainingDocNum = NUM_DOCS - (LOT_SIZE * lots);
    if (remainingDocNum > 0) {
        await db.collection(COLLECTION).insertMany(createDocuments(remainingDocNum))
            .catch(err => {
                console.log("Failed to add documents to collection.");
                client.close();
                throw (err);
            });
    }
    client.close();
    console.log(`Added ${NUM_DOCS} documents to ${COLLECTION} at ${new Date().toJSON()}`);
}

function createDocuments(n) {
    switch (TYPE) {
        case MAIN:
            return createPerformanceTestMain(n);
        case FIELDS:
            return createPerformanceTestFields(n);
        case ARRAY:
            return createPerformanceTestArray(n);
        case RANDOM:
            return createPerformanceTestRandom(n);
        case DEMO:
            return createPerformanceTestDemo(n);
        default:
            throw Error("Invalid document type, must be one of main, fields, array, random, or demo.");
    }
}

function createPerformanceTestMain(n) {
    var documents = [];
    for (var i = 0; i < n; i++) {
        var document = {
            field: "string",
            count: i,
            timestamp: new Date(Date.now()).toISOString(),
            subdocument: {
                field: "ABC",
                field2: [
                    "A", "B", "C"
                ]
            },
            twoLevelArray: [[1, 2], [3, 4], [5, 6]],
            nestedArray: createNestedArray(3)
        };
        var nestedSubdocument = { field: 15 };
        // Creates deeply nested subdocument (with 15 levels)
        for (var j = 14; j >= 0; j--) {
            var newDocument = {
                field: j
            };
            newDocument["subdoc" + j] = nestedSubdocument;
            nestedSubdocument = newDocument;
        }
        document.nestedSubdocument = nestedSubdocument;

        documents.push(document);
    }
    return documents;
}

function createNestedArray(n) {
    var array = [];
    for (var i = 0; i < n; i++) {
        array.push({
            document: i,
            innerArray: [1, 2, 3]
        });
    }
    return array;
}

function createPerformanceTestFields(n) {
    var documents = [];
    for (var i = 0; i < n; i++) {
        var document = {};
        for (var j = 0; j < 1000; j++) {
            document["field" + j] = j;
        }
        documents.push(document);
    }
    return documents;
}

function createPerformanceTestArray(n) {
    var documents = [];
    for (var i = 0; i < n; i++) {
        var innerArray = [];
        for (var j = 0; j < 1000; j++) {
            innerArray.push(j);
        }
        documents.push({ array: innerArray });
    }
    return documents;
}

function createPerformanceTestRandom(n) {
    var documents = [];
    for (var i = 0; i < n; i++) {
        var doc = {};
        for (var j = 0; j < 6; j++) {
            var random = Math.floor(Math.random() * (9));
            switch (random) {
                case 0:
                    doc["field" + j] = 3;
                    break;
                case 1:
                    doc["field" + j] = 3.2;
                    break;
                case 2:
                    doc["field" + j] = 5000000000;
                    break;
                case 3:
                    doc["field" + j] = false;
                    break;
                case 4:
                    doc["field" + j] = "abc";
                    break;
                case 5:
                    doc["field" + j] = null;
                    break;
                case 6:
                    doc["field" + j] = { field: "abc" };
                    break;
                case 7:
                    doc["field" + j] = [1, 2, 3];
                    break;
                case 8:
                    doc["field" + j] = new Date(Date.now()).toISOString();
                    break;
                default:
                    throw Error("Error when randomly generated column, random variable out of bounds.");
            }
        }
        documents.push(doc);
    }
    return documents;
}

/*
 * Creates somewhat realistic data for demo purposes, using Faker to create a person with a list of purchases
 */
function createPerformanceTestDemo(n) {
    var documents = [];

    for (var i = 0; i < n; i++) {
        // Creates random salary between 10000 and 80000, with two decimal places.
        var salary = Math.floor(Math.random() * (7000000) + 1000000) / 100;

        // Creates list of product purchases with fake data.
        var purchases = [];
        var purchasesSize = faker.datatype.number({ min: 0, max: 8 });
        for (var j = 0; j < purchasesSize; j++) {
            purchases.push({
                product: faker.commerce.productName(),
                country: faker.address.country(),
                price: faker.commerce.price(),
                company: faker.company.companyName()
            });
        }

        documents.push(
            {
                name: faker.name.findName(),
                address: {
                    street: faker.address.streetAddress(),
                    postal: faker.address.zipCode(),
                    state: faker.address.state()
                },
                salary: salary,
                purchases: purchases
            }
        );
    }
    return documents;
}
