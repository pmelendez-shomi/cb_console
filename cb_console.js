#!/usr/local/bin/node

var numberOfParameters = 5;

if(((typeof process.argv[2] !== 'undefined') && process.argv[2] == '--help') || (process.argv.length < numberOfParameters))
{
    console.log('Usage: ./cb_console [environment] [bucket] [instruction]');
    process.exit();
}


//var config = require('config');
var repository = require('./dataRepositories/cb');

console.log("cb_console");

var key = "08a10a61-6295-4c0f-8b2b-630315e7b5f2";
var onItem = function (item) {
	console.log('Item:\n    ' + JSON.stringify(item, null, 4));
	process.exit();
};

var onError = function (msg) {
	console.log("[ERROR] | " + msg);
	process.exit();
};
var quit = function () { process.exit(); };

var environment = process.argv[2]  //typeof process.argv[2] !== 'undefined' ? process.argv[2] : 'stg';
var bucket =  process.argv[3];
var instruction = process.argv[4];

var ddocName = "testDesDoc";
var viewName = "testView";

var get = function (key) { repository.getItem(key, onItem, onError, environment, bucket); };
var dd = function () { repository.createView(environment, bucket, "ss")};
var cleantest = function (designDocument) { repository.dropDesignDocument(environment, bucket, ddocName, quit); }; 
var test_query = function (skip) { 
    var cb = function (res, meta) {
        console.log(res, meta, "Done!");
        quit();
    };
    repository.queryView(environment, bucket, ddocName, viewName, cb, skip); 
};
var test = function () {
    var viewdef = { 
        map: function (doc, meta) {
            emit( meta.id , doc);
            }/*,
        reduce: function (keys, values, isRereduce) {

                }*/
    };

    var cb = function (res) {
        console.log("res:", res, "The view was created, now we start a query...");
        repository.queryView(environment, bucket, ddocName, viewName , function(res, meta) { 
            console.log(res,meta,"Done!");
            quit();
            //cleantest();
        });
    };
    repository.createNewView(environment, bucket, ddocName, viewName, viewdef, cb);
};

eval(instruction);
console.log(instruction);
