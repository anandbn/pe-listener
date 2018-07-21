const neo4j = require('neo4j-driver').v1;

function Neo4JUtils() {

    var graphenedbURL = process.env.GRAPHENEDB_BOLT_URL;
    var graphenedbUser = process.env.GRAPHENEDB_BOLT_USER;
    var graphenedbPass = process.env.GRAPHENEDB_BOLT_PASSWORD;

    var driver = neo4j.driver(graphenedbURL, neo4j.auth.basic(graphenedbUser, graphenedbPass));
    var session = driver.session();

    this.runCypherQuery = async function runCypherQuery(query, params) {
        console.log('Executing query :' + query +' with params : '+JSON.stringify(params));
        return session.run(query, params);

    }

    this.upsert = async function upsert(type, findBy, params) {
        //Try to find a match
        try {
            var matchResults = await this.runCypherQuery("MATCH (n:" + type + " {" + findBy + ":$findByKey}) return n", {
                findByKey: params[findBy]
            });
            if (matchResults.records.length == 1) {
                console.log('Found existing ' + type + ' for ' + findBy + '=' + params[findBy]);
                return this.runCypherQuery("MATCH (n:" + type + " {" + findBy + ":$findByKey}) set n=$props return n", {
                    findByKey: params[findBy],
                    props: params
                });

            } else if (matchResults.records.length == 0) {
                console.log('Did not find ' + type + ' for ' + findBy + '=' + params[findBy]);
                return this.runCypherQuery("create (node:" + type + ") set node=$props return node", {
                    props: params
                });
            } else {
                throw new Error('Multiple nodes ' + type + ' for ' + findBy + '=' + params[findBy] + ' found');
            }
        } catch (err) {
            console.log('upsert() :  Error' + err + ',type:' + type + ',findBy:' + findBy + ',findByVal:' + params[findBy]);
            throw err;
        }

    }

    this.upsertRelationship = async function upsertRelationship(source, target, rel) {
        //Try to find a match
        var matchResults = await this.runCypherQuery("MATCH (f)-[rel:" + rel.type + " {" + rel.findBy + ":$findByKey}]->(o) return rel", {
            findByKey: rel.params[rel.findBy]
        });
        if (matchResults.records.length == 1) {
            console.log('Found existing ' + rel.type + ' for ' + rel.findBy + '=' + rel.params[rel.findBy]);
            return this.runCypherQuery("MATCH (f)-[rel:" + rel.type + " {" + rel.findBy + ":$findByKey}]->(o) set rel=$props return rel", {
                findByKey: rel.params[rel.findBy],
                props: rel.params
            });

        } else if (matchResults.records.length == 0) {
            console.log('Did not find ' + rel.type + ' for ' + rel.findBy + '=' + rel.params[rel.findBy]);
            return this.runCypherQuery("match (s:" + source.type + " {" + source.findBy + ":$srcName}) " +
                "match (t:" + target.type + " {" + target.findBy + ":$trgName}) " +
                "create (t)-[rel:" + rel.type + "]->(s) set rel=$props",
                {
                    srcName: source.findByVal,
                    trgName: target.findByVal,
                    props: rel.params
                }
            );
        } else {
            throw new Error("Multiple relationships found ..");
        }

    }

    this.findFieldInObject = async function findFieldInObject(objName, fldName) {
        var fldId;
        try {
            var results = await this.runCypherQuery('match p=allShortestPaths(' +
                '(obj:CustomObject)-[:BelongsTo|RefersTo*0..3]-(fld:CustomField))' +
                ' where obj.name =~ "(?i)' + objName + '" and fld.name =~ "(?i)' + fldName + '"' +
                ' RETURN reduce(theNames =[], n IN nodes(p)| theNames + coalesce(n.name,"")) AS theNames,' +
                'reduce(theNames =[], n IN nodes(p)| theNames + coalesce(n.Id,"")) AS theIds',
                { start: objName, end: fldName });
            for (var i = 0; i < results.records.length; i++) {
                var theNames = results.records[i].get('theNames');
                var theIds = results.records[i].get('theIds');
                if (objName.toUpperCase() === theNames[0].toUpperCase() &&
                    fldName.toUpperCase() === theNames[1].toUpperCase()) {
                    fldId = theIds[theIds.length - 1];
                }

            }

        } catch (error) {
            logger.error(error);
            throw error;
        }
        return fldId;
    }
    this.findObjectName = async function findObjectName(objName, fldName) {
        var fldId;
        try {
            var results = await this.runCypherQuery('match p=allShortestPaths(' +
                '(obj:CustomObject)-[:BelongsTo|RefersTo*0..3]-(fld:CustomField))' +
                ' where obj.name =~ "(?i)' + objName + '" and fld.name =~ "(?i)' + fldName + '"' +
                ' RETURN reduce(theNames =[], n IN nodes(p)| theNames + coalesce(n.name,"")) AS theNames,' +
                'reduce(theNames =[], n IN nodes(p)| theNames + coalesce(n.referenceTo,"")) AS obj_references',
                { start: objName, end: fldName });
            for (var i = 0; i < results.records.length; i++) {
                var theNames = results.records[i].get('theNames');
                var objNames = results.records[i].get('obj_references');
                if (objName.toUpperCase() === theNames[0].toUpperCase() &&
                    fldName.toUpperCase() === theNames[1].toUpperCase()) {
                    fldId = objNames[objNames.length - 1];
                }

            }

        } catch (error) {
            logger.error(error);
            throw error;
        }
        return fldId;
    }
    this.close = async function close() {
        session.close();
        driver.close();
    }
}

module.exports = Neo4JUtils;