require('dotenv').load();
let nforce = require('nforce');
let faye = require('faye');
let Neo4JUtils = require('./neo4jutils.js');
const uuidv1 = require('uuid/v1');
let express = require('express');
let app = express();
let PORT = process.env.PORT || 3000;
let neo4jUtils = new Neo4JUtils();

const POPULAR_CLICK_PATHS = 
'MATCH (start:Page), (end:Page),'+
'path=(start)——(v1:View)<-[:PREV*2]-(v2:View)——(end)'+
'WHERE NONE('+
'v IN NODES(path)[2..LENGTH(path)]'+
'WHERE v.page = start.Name'+
')'+
'AND NONE('+
'v IN NODES(path)[1..LENGTH(path)-1]'+
'WHERE v.page = end.Name'+
')'+
'RETURN EXTRACT(v in NODES(path)[1..LENGTH(path)] | v.page) as path_name, count(path) as view_count '+
'ORDER BY view_count DESC'

const POPULAR_PATHS_FROM_HOME = 
" MATCH (home:Page {Name:'Home:Home'}),"+
" path = (home)<-[:OBJECT]-(home_view)<-[:PREV*3]-(:View)"+
" RETURN EXTRACT(v in NODES(path)[2..LENGTH(path)+1] | v.page) as path_name, count(path) as view_count"+
" ORDER BY count(path) DESC"+
" LIMIT 10;"

const POPULAR_PATHS_FROM_PAGE = 
" MATCH (home:Page {Name:$pageName}),"+
" path = (home)<-[:OBJECT]-(home_view)<-[:PREV*3]-(:View)"+
" RETURN EXTRACT(v in NODES(path)[2..LENGTH(path)+1] | v.page) as path_name, count(path) as view_count"+
" ORDER BY count(path) DESC"+
" LIMIT 10;"

let popularPaths = async function(req, res) {
    let results = await neo4jUtils.runCypherQuery(POPULAR_CLICK_PATHS);
    let restResult = new Array();
    for(let i=0;i<results.records.length;i++){
        restResult.push({
            "path":results.records[i].get('path_name').join('->'),
            "viewCount":results.records[i].get('view_count').low
        })
    }
    // We must end the request when we are done handling it
    res.json(restResult);
  };
let pathsFromHome =  async function(req, res) {
    req.params.pageName='Home:Home';
    let restResult = await pathsFromPage(req,res);
    res.json(restResult);
  };
let pathsFromPage = async function(req, res) {
    page = req.params.pageName;
    
    let results = await neo4jUtils.runCypherQuery(POPULAR_PATHS_FROM_PAGE, {"pageName":page});
    let restResult = new Array();
    for(let i=0;i<results.records.length;i++){
        restResult.push({
            "path":results.records[i].get('path_name').join('->'),
            "viewCount":results.records[i].get('view_count').low
        })
    }
    // We must end the request when we are done handling it
    res.json(restResult);
  }
app.get('/popular-paths',popularPaths );
app.get('/popular-paths-from-home',pathsFromHome);
app.get('/popular-paths-from-page/:pageName',pathsFromPage);
let server = require('http').Server(app);
let io = require('socket.io')(server);
const SESSION_THRESHOLD_MINS = 30;


var event = { "schema": "v1QkPpwTJ_Xp7dJMFpiK2Q", "payload": { "CreatedDate": "2018-05-27T15:58:53Z", "CreatedById": "00541000000KNCqAAO", "Object__c": null, "Record_Id__c": null, "Type__c": "Home" }, "event": { "replayId": 107 } }

async function storePageView(eventPayload) {
    var currDate = new Date();
    console.log('Current Date:'+currDate.toISOString());
    var prevPageThreshold = currDate.getTime() - (SESSION_THRESHOLD_MINS*60*1000)
    //Upsert the user
    var cypRes = await neo4jUtils.upsert('User',
        'Id',
        { 'Id': eventPayload.payload.CreatedById }
    );
    console.debug('Created  ' + cypRes.summary.counters._stats.nodesCreated + ' User nodes');

    //Upsert the page
    var pageName = eventPayload.payload.Type__c;
    if (eventPayload.payload.Object__c) {
        pageName += ':' + eventPayload.payload.Object__c;
    }
    cypRes = await neo4jUtils.upsert('Page',
        'Name',
        { 'Name': pageName }
    );
    console.debug('Created  ' + cypRes.summary.counters._stats.nodesCreated + ' Page nodes');

    //Create a page View
    let reqId = uuidv1();
    cypRes = await neo4jUtils.upsert('View',
        'Id',
        { 'Id': reqId,
          'recordId' : eventPayload.payload.Record_Id__c,
          'CreatedDate': eventPayload.payload.CreatedDate,
          'CreatedDateMillis': Date.parse(eventPayload.payload.CreatedDate),
          'page':pageName,
          'UserId':eventPayload.payload.CreatedById
        }
    );
    console.debug('Created  ' + cypRes.summary.counters._stats.nodesCreated + ' View nodes');

    var matchResults = await neo4jUtils.runCypherQuery('match (u:User)--(pv:View) '+
                                                       'where pv.CreatedDateMillis>=$threshold and u.Id=$userId '+
                                                       'return pv.Id order by pv.CreatedDateMillis desc limit 1',
                                            {
                                                'threshold':prevPageThreshold,
                                                'userId':eventPayload.payload.CreatedById
                                            });

    if (matchResults.records.length == 1) {
        var prevPageId = matchResults.records[0].get('pv.Id');
        console.log('Found previous page within threshold Id:'+prevPageId);

        //Create Page to Previous Page relationship
        cypRes = await neo4jUtils.upsertRelationship(
            //source
            {
                type: "View",
                findBy: "Id",
                findByVal: prevPageId 
            },
            //target
            {
                type: "View",
                findBy: "Id",
                findByVal: reqId
            },
            //relationship
            {
                type: "PREV",
                findBy: "Id",
                params: {
                    Id: reqId+'-PreviousPage',
                }
            }
        );
    } else{
        console.log(' Did not find Previous page within threshold ...');
    }                       
    //Create user-pageview relationship
    cypRes = await neo4jUtils.upsertRelationship(
        //source
        {
            type: "View",
            findBy: "Id",
            findByVal: reqId
        },
        //target
        {
            type: "User",
            findBy: "Id",
            findByVal: eventPayload.payload.CreatedById 
        },
        //relationship
        {
            type: "VERB",
            findBy: "Id",
            params: {
                Id: eventPayload.payload.CreatedById +'-'+reqId,
            }
        }
    );
    console.debug('Created  ' + cypRes.summary.counters._stats.relationshipsCreated + ' User-View relationships');

    //Create PageView-Page relationship
    cypRes = await neo4jUtils.upsertRelationship(
         //source
         {
            type: "Page",
            findBy: "Name",
            findByVal: pageName
        },
        //target
        {
            type: "View",
            findBy: "Id",
            findByVal: reqId
        },
        //relationship
        {
            type: "OBJECT",
            findBy: "Id",
            params: {
                Id: eventPayload.payload.CreatedById +'-'+reqId,
            }
        }
    );
    console.debug('Created  ' + cypRes.summary.counters._stats.relationshipsCreated + ' User-PageView relationships');
}

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
        storePageView(message);
        console.log('Platform event Bread_Crumb__e received ....' + JSON.stringify(message));
        // Send message to all connected Socket.io clients
        //io.of('/').emit('bread_crumb', message);
    });

};
