/*********************************************************************************
Dependencies
**********************************************************************************/
var config 		= require('config');
var couchbase 	= require('couchbase');
//var logger 		= require('../logger').getLogger();
/*********************************************************************************/

/**********************************************************************************
Config
**********************************************************************************/
var cbBucket    = null; 
/*********************************************************************************/

/**********************************************************************************
Module
**********************************************************************************/
exports.addItem =  function (id, session, expiryInSec, onSuccess, onError) {
    upsertSession(id, session, expiryInSec, onSuccess, onError);
};

exports.getItem = function(id, onSuccess, 
		onError, env, bucket) {

    validateKeyLength(id);
    
    connectBucket(env, bucket, function(bucket)  { bucket.get(
        id, 
        {},
        function(err, response) {
            if (err) {
                onError(err);
            } else {
                var session = deleteJsonType(response.value);
                onSuccess(session);
            }
    });}); 
};

/*exports.updateItem = function(id, session, expiryInSec, onSuccess, onError) {
    upsertSession(id, session, expiryInSec, onSuccess, onError);
};

exports.deleteItem = function(id, onSuccess, onError) {
    validateKeyLength(id);
    connectBucket().remove(
        id, 
        function(err, response) {
            if (err) {
                onError(err);
            } else {
                onSuccess();
            }
    }); 
};
*/

/*********************************************************************************/
/*function upsertSession(id, session, expiryInSec, onSuccess, onError) {

    validateKeyLength(id);
    session = addJsonType(session, config.couchbase.jsonTypeSession);
    
    try {
        connectBucket().upsert(
            id, 
            session,
            { expiry: getCbExpiry(expiryInSec) },
            function(err, response){
                if (err) {
                    onError(err);
                } else {
                    session = deleteJsonType(session);
                    onSuccess(session);
                }
            }
        );
    } catch (error) {
        onError(error);
    }
}*/

function addJsonType(doc, jsonType) {
    doc.JsonType = jsonType;
    return doc;
}

function deleteJsonType(doc) {
    delete doc["JsonType"];
    return doc;
}

function getEnv(env)
{
    var myEnv;
    if(env == "prod")
    {
        myEnv = config.couchbase;
    }
    else
    {
        myEnv = config['couchbase_' + env];
    }
    return myEnv;
}

exports.dropDesignDocument = function (env, bucket, dd_name, callback) {
	
	var bucketCallback = function (cbBucket) {
            var manager = cbBucket.manager();
            manager.removeDesignDocument(dd_name, function (err) { logError(err); console.log("Done with removing design doc: " + dd_name); callback(); });    
	};

	connectBucket(env, bucket, bucketCallback);
}

exports.createDesignDocument = function (env, bucket, name, designDoc, doneCallback) {
    internalCreateDDoc(env, bucket, name, designDoc, doneCallback);
}

exports.createNewView = function (env, bucket, newDesignDocName, viewName, viewDef, callback) {

    var dd = {
        language: 'javascript',
        views: { }
    };

    dd.views[viewName] = viewDef;//{ map: mapFunction };

    internalCreateDDoc(env, bucket, newDesignDocName, dd, callback);
}

exports.queryView = function (env, bucket, designDocName, viewName, callback, skip) {
    var cbBucketCB = function (cbBucket) {
        var skipVal = skip || 0;
        var query = couchbase.ViewQuery.from(designDocName, viewName).stale(couchbase.ViewQuery.Update.BEFORE).skip(skipVal).limit(1); //couchbase.ViewQuery.Update.NONE
        //var query = couchbase.ViewQuery.from(designDocName, viewName);
        cbBucket.query(query, null, function(err, res, meta) { logError(err); callback(res,meta); } );
    }

    connectBucket(env, bucket, cbBucketCB);
}

function internalCreateDDoc(env, bucket, name, designDoc, doneCallback)
{
    var bucketcb = function (cbBucket) {
        var manager = cbBucket.manager();
        manager.insertDesignDocument(name, designDoc, function (err, res) { logError(err); doneCallback(res);  } );
    };
    
    connectBucket(env, bucket, bucketcb);
}

function logError(err)
{
    if(err)
    {
        console.log(err);
        process.exit();
    }
}

function connectBucket(env, bucket, callback) {
    var myEnv = getEnv(env);

   
    if (cbBucket == null || cbBucket.connected != true) {
        
        var host = process.env.CBHOST || myEnv.host;
        var bucketName = process.env.CBBUCKET || bucket;//myEnv.bucket;
        var bucketPwd = process.env.CBPASSWORD || myEnv[bucket + '_password']//.password;
        var msg = 'Connecting to: {host}/{bucket}'
                    .replace('{host}', host)
                    .replace('{bucket}', bucketName);
        console.log(msg);
	//console.log(bucketName + "|" + bucketPwd);
        
        try {
            var cbCluster   = new couchbase.Cluster(host);
            cbBucket = cbCluster.openBucket(bucketName, bucketPwd, function(err) { 
                logError(err); 
                //cbBucket.operationTimeout = 120*1000; 
                callback(cbBucket); });
                                
        } catch (err) {
            console.log("[Error] " + err);
        }
        
    }
    else {
        console.log("The bucket is already set up. Returning existing");
        callback(cbBucket); 
    }
}

function validateKeyLength(id) {
    if (id.length > 250)
        throw new Error('Key is longer than 250 bytes');
}

//http://docs.couchbase.com/developer/dev-guide-3.0/doc-expiration.html
function getCbExpiry(expirySec){
    if (expirySec <= 2592000)
        return expirySec;
    else {
        var t = new Date();
        t.setSeconds(t.getSeconds() + expirySec);
        t = t.getTime() / 1000;
        return t;  
    } 
}

