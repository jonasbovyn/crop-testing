const QueryEngineFactory = require('@comunica-crop/engines/query-sparql').QueryEngineFactory;
const fs = require('fs/promises');

// Run with --experimental-wasi-unstable-preview1


const replication = 20;



const query = 'SELECT * WHERE {' +
    '?v0 <http://xmlns.com/foaf/homepage> ?v1 .' +
    '?v0 <http://ogp.me/ns#title> ?v1 . ' +
    '}';

const sources = [`http://localhost:5000/data100k`];

async function run() {
    let times = []

    for (let i = 0; i < replication; i++) {

        const engine = await new QueryEngineFactory().create({
            configPath: `config/engines/server/config-crop.json`,
        });

        const initTime =  (id, elapsedTime) => {
            if (id === "optimization-time") {
                times.push(elapsedTime);
                console.log(elapsedTime);
            }
        };

        await engine.queryBindings(query, {
            sources: sources,
            benchmarkTimeLog: initTime,
            overrideCropSettings: { k: 2, skipEval: true, mode: 'wasm' }}
        );

    }

    const mean = times.reduce((a, b) => a + b, 0) / times.length;

    console.log(`Instantiating WASM took ${mean}ms`);

}
run().then(() => {});
