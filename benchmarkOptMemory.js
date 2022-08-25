const QueryEngineFactory = require('@comunica-crop/engines/query-sparql').QueryEngineFactory;
const fs = require('fs/promises');

// https://stackoverflow.com/questions/20018588/how-to-monitor-the-memory-usage-of-node-js
// https://stackoverflow.com/questions/12023359/what-do-the-return-values-of-node-js-process-memoryusage-stand-for

// Run with --experimental-wasi-unstable-preview1 --expose-gc
// Run on memory benchmark branch of CROP

const dataset = "10M"

const maxK = 10;
const maxQuerySize = 50;

// const maxQueryTime = 5000;
const maxMemUsage = 50000000;
const replication = 10;

const modes = [
    {mode: 'wasm', name: 'wasm'},
    {mode: 'js', name: 'js'}
];



const directory = "datasets/dataset" + dataset;
const path = `${__dirname}/${directory}`;
const outputFile = `${path}/optimization-memory-${Date.now()/1000}.csv`;


predicates = [
    "<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/gender>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/hits>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/likes>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate>",
    "<http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor>",
    "<http://ogp.me/ns#tag>",
    "<http://ogp.me/ns#title>",
    "<http://purl.org/dc/terms/Location>",
    "<http://purl.org/goodrelations/includes>",
    "<http://purl.org/goodrelations/offers>",
    "<http://purl.org/ontology/mo/artist>",
    "<http://purl.org/ontology/mo/conductor>",
    "<http://purl.org/stuff/rev#hasReview>",
    "<http://purl.org/stuff/rev#reviewer>",
    "<http://purl.org/stuff/rev#title>",
    "<http://purl.org/stuff/rev#totalVotes>",
    "<http://schema.org/actor>",
    "<http://schema.org/caption>",
    "<http://schema.org/contentRating>",
    "<http://schema.org/contentSize>",
    "<http://schema.org/description>",
    "<http://schema.org/eligibleRegion>",
    "<http://schema.org/jobTitle>",
    "<http://schema.org/keywords>",
    "<http://schema.org/language>",
    "<http://schema.org/legalName>",
    "<http://schema.org/nationality>",
    "<http://schema.org/publisher>",
    "<http://schema.org/text>",
    "<http://schema.org/trailer>",
    "<http://schema.org/url>",
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
    "<http://xmlns.com/foaf/age>",
    "<http://xmlns.com/foaf/familyName>",
    "<http://xmlns.com/foaf/givenName>",
    "<http://xmlns.com/foaf/homepage>",
]

function randomPredicate() {
    return predicates[Math.floor((Math.random()*predicates.length))];
}


function createRandomQuery(triples) {
    // doesn't matter how real the query is, only used for optimization benchmark purpose
    // random predicates are used because different estimated cardinalities are still desirable

    let query = "SELECT * WHERE {\n";
    for (let i = 0; i < triples; i++) {
        query += `\t?v0 ${randomPredicate()} ?v1 .\n`
    }
    query += "}"
    return query;
}


async function measureQuery(query, engine, sources, k, mode) {
    return new Promise(async (resolve, reject) => {
        const memUsage =  (id, mem) => {
            if (id === "crop-memory") {
                resolve(mem);
            }
        };
        await engine.queryBindings(query, {
            sources: sources,
            benchmarkTimeLog: memUsage,
            overrideCropSettings: { k: k, skipEval: true, mode: mode }}
        );
    });
}


async function writeResults(results) {
    function convertToCSV(arr) {
        const array = [Object.keys(arr[0])].concat(arr)
        return array.map(it => {
            return Object.values(it).toString()
        }).join('\n')
    }

    await fs.writeFile(outputFile, convertToCSV(results));
}

async function run() {

    const cropEngine = await new QueryEngineFactory().create({
        configPath: `config/engines/server/config-crop.json`,
    });

    const sources = [`http://localhost:5000/data${dataset}`];


    const results = [];

    for (let {mode, name} of modes) {
        for (let k = 2; k <= maxK; k++) {
            for (let querySize = 2; querySize <= maxQuerySize; querySize++) {
                console.log(`mode = ${mode}, k = ${k}, size = ${querySize}`)


                let memories = []
                // memory usage is consistent when using WASM, not JS(?)
                for (let i = 0; i < (mode === 'wasm' ? 1 : replication); i++) {
                    let memory = await measureQuery(createRandomQuery(querySize), cropEngine, sources, k, mode);
                    memories.push(memory)
                }

                const memory = memories.reduce((a, b) => a + b, 0) / memories.length;

                console.log(`Memory usage: ${memory}`);

                const result = {
                    triples: querySize,
                    k: k,
                    mode: name,
                    memory_usage: memory / 1000
                }

                results.push(result);

                if (memory > maxMemUsage) {
                    break;
                }
            }
        }
    }

    await writeResults(results);

}

run().then(() => {});
