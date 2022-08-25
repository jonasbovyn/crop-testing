const QueryEngineFactory = require('@comunica-crop/engines/query-sparql').QueryEngineFactory;
const fs = require('fs/promises');

// Run with --experimental-wasi-unstable-preview1

const dataset = "10M"

const pre_warmup = 1000;

const warmup = 2;
const replication = 100;
const filter_outliers = 0.2 // neglects outliers, takes mean of the rest

const maxK = 10;
const maxQuerySize = 50;

const maxQueryTime = 5000; // after that, querySize stops counting up

const modes = ['js', 'wasm'];



const directory = "datasets/dataset" + dataset;
const path = `${__dirname}/${directory}`;
const outputFile = `${path}/optimization-${warmup}-${replication}-${Date.now()/1000}.csv`;


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


async function timeQuery(query, engine, sources, k, mode) {
    return new Promise(async (resolve, reject) => {

        let calculationTime = undefined;

        const idpTime =  (id, elapsedTime) => {
            if (id === "crop-idp") {
                calculationTime = elapsedTime;
                resolve(calculationTime);
            }
        };

        const bindingsStream = await engine.queryBindings(query, {
            sources: sources,
            benchmarkTimeLog: idpTime,
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


    for (let mode of modes) {

        for (let i = 0; i < pre_warmup; i++) {
            await timeQuery(createRandomQuery(10), cropEngine, sources, 2, mode);
        }

        for (let k = 2; k <= maxK; k++) {

            for (let querySize = 2; querySize <= maxQuerySize; querySize++) {
                console.log(`mode = ${mode}, k = ${k}, size = ${querySize}`)

                for (let i = 0; i < warmup; i++) {
                    await timeQuery(createRandomQuery(querySize), cropEngine, sources, k, mode);
                }

                let times = []
                for (let i = 0; i < replication; i++) {
                    let time = await timeQuery(createRandomQuery(querySize), cropEngine, sources, k, mode);
                    times.push(time)
                }

                let sorted = times.sort((a,b) => a - b);
                let neglect = Math.ceil(filter_outliers * sorted.length);
                let filtered = sorted.slice(neglect, replication - neglect);

                const optimizationTime = filtered.reduce((a, b) => a + b, 0) / filtered.length;


                const result = {
                    triples: querySize,
                    k: k,
                    mode: mode,
                    optimization_time: optimizationTime
                }

                results.push(result);

                if (optimizationTime > maxQueryTime) {
                    break;
                }
            }
        }
    }

    await writeResults(results);

}

run().then(() => {});
