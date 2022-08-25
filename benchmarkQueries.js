const QueryEngineFactory = require('@comunica-crop/engines/query-sparql').QueryEngineFactory;
const fs = require('fs/promises');

// Run with --experimental-wasi-unstable-preview1

const dataset = "100k"


const useServer = true; // uses file as datasource otherwise

const directory = "datasets/dataset" + dataset;
const warmup = 2;
const replication = 10;

// if query execution takes longer, don't replicate.
// Since replication is more relevant for getting precise values that are in the low range, and since we visualize on a log scale
// For higher values it does not affect conclusions and can take up quite some time
const skipReplicationThreshold = 50000;
const timeout = 900000;

const minimumTriples = 0;
const maximumTriples = 100;

const path = `${__dirname}/${directory}`;
const outputFile = `${path}/benchmarks-${warmup}-${replication}-${Date.now()/1000}.csv`;

// Specify the outputFile if you want to continue an existing benchmark (comunica bindjoin seems to have a memory leak after long execution)
// const outputFile = `${path}/benchmarks-1-3-1659832037.261.csv`;

// Don't perform a query evaluation if the value is "k" is higher than the amount of triples in the query
// The value of "k" would be automatically reduced to the amount of triples in the optimizer
const skipTooHighKValues = false;

async function run() {
    async function timeQuery(query, engine, sources, settings) {
        return new Promise(async (resolve, reject) => {

            let calculationTime = undefined;

            const idpTime =  (id, elapsedTime) => {
                if (id === "crop-idp") {
                    calculationTime = elapsedTime;
                }
            };

            const start = process.hrtime.bigint();
            const bindingsStream = await engine.queryBindings(query, {sources: sources, benchmarkTimeLog: idpTime, overrideCropSettings: settings});
            let resultCount = 0;
            let resultsTimestamps = [];

            function onMaybeTimeout() {
                if (!bindingsStream.ended) {
                    bindingsStream.destroy();
                    reject('Timeout');
                }
            }

            let timeOutId = setTimeout(onMaybeTimeout, timeout);

            bindingsStream.on('data', (binding) => {
                // resultsTimestamps.push(Date.now() - start);
                resultCount += 1;
            });
            bindingsStream.on('end', () => {
                const end = process.hrtime.bigint();
                // Cut off macroseconds
                const elapsedTimeBigint = (end - start) / BigInt(1_000);
                // Convert to miliseconds with decimal point
                const totalTime = Number(elapsedTimeBigint) / 1_000;

                clearTimeout(timeOutId);
                resolve([resultsTimestamps, totalTime, calculationTime, resultCount]);
            });
            bindingsStream.on('error', (error) => {
                console.log(error);
                reject(error);
            });
        });
    }

    async function writeResults(benchmarkResults) {
        function convertToCSV(arr) {
            const array = [Object.keys(arr[0])].concat(arr)
            return array.map(it => {
                return Object.values(it).toString()
            }).join('\n')
        }

        await fs.writeFile(outputFile, convertToCSV(benchmarkResults));
    }

    async function isQuerySetAlreadyEvaluated(querySetName, engineName, results) {
        for (let object of results) {
            if (object['set'] === querySetName && object['engine'] === engineName) {
                return true;
            }
        }
    }

    async function initialResults() {
        function csvToArray(csv) {
            // https://stackoverflow.com/questions/28543821/convert-csv-lines-into-javascript-objects
            const jsonObj = [];
            const headers = csv[0].split(',');
            for(let i = 1; i < csv.length; i++) {
                const data = csv[i].split(',');
                const obj = {};
                for(let j = 0; j < data.length; j++) {
                    obj[headers[j].trim()] = data[j].trim();
                }
                jsonObj.push(obj);
            }
            return jsonObj
        }

        try {
            const csv = (await fs.readFile(outputFile, 'utf8')).split("\n");
            return csvToArray(csv);
        }
        catch (err) {
            // File doesn't exist
            return [];
        }
    }

    async function benchmarkQueries(engines, querySets, sources) {
        const results = await initialResults();

        for (let {querySet, querySetName} of querySets) {

            for (let {engine, engineName, settings} of engines) {

                if (await isQuerySetAlreadyEvaluated(querySetName, engineName, results)) {
                    console.log(`Skipping ${querySetName} for ${engineName}: already done`)
                    continue;
                }

                try {
                    const querySetResults = [];

                    // Complex queries do not contain placeholders, leading to one distinct query in C1,
                    // C2, and C3. - CROP
                    const querySetLength = querySetName.startsWith("C") ? 1 : querySet.length;

                    for (let id = 0; id < querySetLength; id++) {
                        const query = querySet[id];
                        const triples = (query.match(/\n/g)||[]).length - 1;

                        if (triples < minimumTriples || triples > maximumTriples ||
                            (skipTooHighKValues && settings !== undefined && settings.k > triples)) {
                            continue;
                        }

                        if (id === 0) {
                            for (let i = 0; i < warmup; i++) {
                                await timeQuery(query, engine, sources, settings);
                            }
                        }

                        let [timestamps, totalTime, calcTime, totalFound] = await timeQuery(query, engine, sources, settings);

                        if (totalTime < skipReplicationThreshold) {
                            for (let i = 0; i < replication - 1; i++) {
                                let [timestamps2, totalTime2, calcTime2, totalFound2] = await timeQuery(query, engine, sources, settings);
                                for (let j = 0; j < timestamps.length; j++) {
                                    timestamps[j] += timestamps2[j];
                                }
                                totalTime += totalTime2;
                                totalFound += totalFound2;
                                if (calcTime !== undefined) {
                                    calcTime += calcTime2;
                                }
                            }

                            for (let j = 0; j < timestamps.length; j++) {
                                timestamps[j] /= replication;
                            }
                            totalTime /= replication;
                            totalFound /= replication;
                            if (calcTime !== undefined) {
                                calcTime /= replication;
                            }
                        }

                        let timestampsJoined = timestamps.join(" ");

                        console.log(`Finished ${querySetName}-${id} for ${engineName}`);

                        const resultRecord = {
                            engine: engineName,
                            set: querySetName,
                            dataset: dataset,
                            k: settings === undefined ? -1 : (settings.k === undefined ? -1 : settings.k),
                            id: id,
                            triples: triples,
                            results: totalFound,
                            calcTime: calcTime,
                            execTime: totalTime - (calcTime === undefined ? 0 : calcTime),
                            totalTime: totalTime,
                            error: "",
                            timestamps: timestampsJoined
                        };
                        console.log(`${engineName} - calctime: ${resultRecord.calcTime} - totaltime: ${resultRecord.totalTime}`)
                        querySetResults.push(resultRecord);
                    }

                    results.push(...querySetResults);
                }
                catch (e) {
                    const resultRecord = {
                        engine: engineName,
                        set: querySetName,
                        dataset: dataset,
                        k: settings === undefined ? -1 : (settings.k === undefined ? -1 : settings.k),
                        id: -1,
                        triples: -1,
                        results: -1,
                        calcTime: -1,
                        execTime: timeout,
                        totalTime: timeout,
                        error: (e+"").split(":")[0],
                        timestamps: ''
                    };
                    results.push(resultRecord);
                    console.log(`Timed out ${querySetName} for ${engineName}`);
                }

                await writeResults(results);
            }

        }

        return results;
    }

    const comunicaEngine = await new QueryEngineFactory().create({
        configPath: `config/engines/${useServer ? 'server' : 'file'}/config-comunica.json`,
    });
    const comunicaNoBindEngine = await new QueryEngineFactory().create({
        configPath: `config/engines/${useServer ? 'server' : 'file'}/config-comunica-NLJSHJ.json`,
    });
    const cropEngine = await new QueryEngineFactory().create({
        configPath: `config/engines/${useServer ? 'server' : 'file'}/config-crop.json`,
    });


    const createCropEngine = (k, mode) => {
        return {engine: cropEngine, engineName: `crop-${mode}-${k}`, settings: {k: k, mode: mode}}
    }

    const engines = [
        {engine: comunicaEngine, engineName: "comunica"},
        // {engine: comunicaNoBindEngine, engineName: "comunica-flat"},
        createCropEngine(3, 'js'),
        createCropEngine(3, 'wasm'),
        // createCropEngine(2, 'wasm'),
        // createCropEngine(3, 'wasm'),
        // createCropEngine(4, 'wasm'),
        // createCropEngine(5, 'wasm'),
        // createCropEngine(6, 'wasm'),
        // createCropEngine(7, 'wasm'),
        // createCropEngine(8, 'wasm'),
        // createCropEngine(9, 'wasm'),
        // createCropEngine(10, 'wasm'),
    ];

    const sources = [];
    if (!useServer) {
        sources.push(`${path}/dataset.nt`);
    }
    else {
        sources.push(`http://localhost:5000/data` + dataset);
    }

    const querySets = [];
    const dir = await fs.opendir(`${path}/queries`)
    for await (const dirent of dir) {
        const querySetName = dirent.name.split(".")[0];
        const data = await fs.readFile(`${path}/queries/${querySetName}.txt`);
        const queriesRaw = data.toString();
        const queries = queriesRaw.split("\n\n").map(s => s.replaceAll("\t", ""));
        querySets.push({
            querySetName,
            querySet: queries
        })
    }

    const benchmarkResults = await benchmarkQueries(engines, querySets, sources);
}

run().then(() => {});
