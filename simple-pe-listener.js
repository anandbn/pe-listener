let nforce = require('nforce');
let faye = require('faye');
const uuidv1 = require('uuid/v1');
let express = require('express');
let app = express();
let server = require('http').Server(app);
let io = require('socket.io')(server);
const SESSION_THRESHOLD_MINS = 30;

let PORT = process.env.PORT || 3000;

let bayeux = new faye.NodeAdapter({ mount: '/faye', timeout: 45 });
bayeux.attach(server);
bayeux.on('disconnect', function (clientId) {
    console.log('Bayeux server disconnect');
});

server.listen(PORT, () => console.log(`Express server listening on ${PORT}`));

// Connect to Salesforce
let SF_CLIENT_ID = process.env.SF_CLIENT_ID;
let SF_CLIENT_SECRET = process.env.SF_CLIENT_SECRET;
let SF_USER_NAME = process.env.SF_USER_NAME;
let SF_USER_PASSWORD = process.env.SF_USER_PASSWORD;
let SF_SECURITY_TOKEN = process.env.SF_SECURITY_TOKEN;

console.log('SF_CLIENT_ID:' + SF_CLIENT_ID);
console.log('SF_CLIENT_SECRET:' + SF_CLIENT_SECRET);
console.log('SF_USER_NAME:' + SF_USER_NAME);
console.log('SF_USER_PASSWORD:' + SF_USER_PASSWORD);
console.log('SF_SECURITY_TOKEN:' + SF_SECURITY_TOKEN);


let org = nforce.createConnection({
    clientId: SF_CLIENT_ID,
    clientSecret: SF_CLIENT_SECRET,
    environment: "production",
    redirectUri: 'http://localhost:3000/oauth/_callback',
    mode: 'single',
    autoRefresh: true
});

org.authenticate({ username: SF_USER_NAME, password: SF_USER_PASSWORD, securityToken: SF_SECURITY_TOKEN }, err => {
    if (err) {
        console.error("Salesforce authentication error");
        console.error(err);
    } else {
        console.log("Salesforce authentication successful");
        console.log(org.oauth.instance_url);
        subscribeToPlatformEvents();
    }
});

// Subscribe to Platform Events
let subscribeToPlatformEvents = () => {
    var client = new faye.Client(org.oauth.instance_url + '/cometd/40.0/');
    client.setHeader('Authorization', 'OAuth ' + org.oauth.access_token);
    client.subscribe('/event/Bread_Crumb__e', function (message) {
        console.log('Platform event Bread_Crumb__e received ....' + JSON.stringify(message));
        // Send message to all connected Socket.io clients
        //io.of('/').emit('bread_crumb', message);
    });

};
