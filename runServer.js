const runServer = require('@ldf/core').runCustom;

runServer(["config/config-server.json", "5000", "20"],
    process.stdin, process.stdout, process.stderr,
    null, { mainModulePath: __dirname + "/node_modules/@ldf/server" }
);
