/**
 * Parses an object structure, flattens it
 * and sends the whitelisted metrics to statsd
 * Depends implicitily on statsd client
 **/

'use strict';

const DOT = '.';

/**
 * Default filter function, can be overriden when instantiating module
 **/
function defaultFilterFn(key) {
    const defaultWhitelist = [
        // Broker stats
        'outbuf_cnt',
        'outbuf_msg_cnt',
        'waitresp_cnt',
        'waitresp_msg_cnt',
        'tx',
        'txbytes',
        'txerrs',
        'txretries',
        'req_timeouts',
        'rx',
        'rxbytes',
        'rxerrs',
        'rxcorriderrs',
        'rxpartial',
        'rtt',
        'throttle',

        // Topic partition stats
        'msgq_cnt',
        'msgq_bytes',
        'xmit_msgq_cnt',
        'xmit_msgq_bytes',
        'fetchq_cnt',
        'fetchq_size',
        'query_offset',
        'next_offset',
        'app_offset',
        'stored_offset',
        'committed_offset',
        'eof_offset',
        'lo_offset',
        'hi_offset',
        'consumer_lag',
        'txmsgs',
        'txbytes',
        'msgs',
        'rx_ver_drops'
    ];


    return defaultWhitelist.indexOf(key) >= 0;
}


function isNil(item) {
    // catching both null and undefined, jshint doesn't let us do ==
    return item === null || item === undefined;
}

function isArray(obj) {
    return !isNil(obj) && Array.isArray(obj);
}


function isObject(obj) {
    return !isNil(obj) && obj.constructor === Object;
}

function isFunction(obj) {
    return !isNil(obj) && obj.constructor === Function;
}

// we assume that if it is not another object or array
// it is a primitive type
// obviously not true in javascript at large but true for this module usage
function isPrimitive(item) {
    return !isObject(item) && !isArray(item);
}

function isString(item) {
    // gotcha string objects need to be converted to string
    // primitive types, valueof does that
    return typeof item.valueOf() === "string";

}

function isNumber(item) {
    var amount = Number(item);
    return !Number.isNaN(amount) && Number.isFinite(amount);

}

function cleanUnwantedChars(str) {
    const UNDERSCORE = '_';
    return str.replace(/:|\.|;|\//g, UNDERSCORE);
}

/**
 * Recurses through objects and flattens them
 * into a single level dict of key: value pairs.  Each
 * key consists of all of the recursed keys joined by
 * separator that is hardcoded to be a dot.
 *
 **/
function flatten(obj) {

    var dict = {};

    // return empty object, raising an error seems like
    // it would polute the logs
    if (!isObject(obj) && !isArray(obj)) {
        return dict;
    }

    // looks like metric obj is really sent as a string
    if ('message' in obj) {
        obj = JSON.parse(obj.message);
    }


    function _flatten(obj, dict, keyPrefix) {

        // object has -ad minimum - a key, value pair
        // {'some-key': 'some-value-maybe-another-object'}
        // decide if object or array

        Object.keys(obj).forEach(function (name) {
            var keyName;

            var cleanName = cleanUnwantedChars(name);

            if (keyPrefix) {
                keyName = keyPrefix + DOT + cleanName;
            } else {
                keyName = cleanName;
            }

            if (isPrimitive(obj[name])) {
                dict[keyName] = obj[name];
            } else {
                // continue recursing
                _flatten(obj[name], dict, keyName);

            }

        });


        return dict;
    }
    return _flatten(obj, dict, null);
}

/**
 * Removes unwanted keys from data object
 * Object must be fully built as the keys you are interested on might be deep
 * in the structure.
 *
 */
function filterKeys(data, filterFn) {

    var filteredData = {};

    // these values are nonsensical and always filtered regardless
    // of passed in filter function
    const blacklist = ['toppars', '-1'];


    Object.keys(data).forEach(function (name) {
        // split property by '.'
        var items = name.split(DOT);
        //check blacklist, takes precedence
        var blacklisted = false;
        for (var j = 0; j < items.length; j++) {
            if (blacklist.indexOf(items[j]) >= 0) {
                blacklisted = true;
                break;
            }
        }
        // if any of the items appears in filterFn keep property on object
        // unless this key is alredy blacklisted
        for (var i = 0; !blacklisted && i < items.length; i++) {

            if (filterFn(items[i])) {
                filteredData[name] = data[name];
                break;
            }
        }

    });

    return filteredData;
}

/**
 * Reports metrics to statsd using reporter
 * returns data for convenience, but not really needed
 * Please take a look at https://blog.pkhamre.com/understanding-statsd-and-graphite/
 * to learn about graphite metrics.
 *
 * Gauge metrics represent a 'an arbitrary (numeric) value at a point in time'
 * and this is what kafka metrics will be most often.
 *
 **/
function report(data, reporter) {

    Object.keys(data).forEach(function (metric) {
        if (isNumber(data[metric])) {

            try {
                reporter.gauge(metric, data[metric]);
            } catch (e) {
                //nothing you can do
            }
        }

    });
    return data;
}

/**
 * Returns a function that parses an object and sents its key/value pairs as metric/value
 * to graphite.
 *
 * Given a raw object it will be traversed, and each leaf node of the object will
 * be keyed by a concatenated key made up of all parent keys.
 * Keys are the name of metrics that are sent to graphite
 * Clients have the option of filtering metrics and only sent the ones they care about
 * by supplying a filtering function that given a string (metric name) returns true or false
 * A default whitelist is provided
 *
 * If filterFn === false no whitelist is applied
 *
 **/

function createRdKafkaStatsCb(reporter, options) {
    var optionalWhitelistFn;

    options = options || {};


    // use default filtering if none is passed in
    if (!('filterFn' in options)) {
        options.filterFn = defaultFilterFn;
    } else if (options.filterFn === false) {
        // no filtering
        return function (rawObject) {
            return report(flatten(rawObject), reporter);

        };
    } else if (!isFunction(options.filterFn)) {
        throw new Error('Must provide a function for options.filterFn');
    }


    return function (rawObject) {
        return report(filterKeys(flatten(rawObject), options.filterFn), reporter);
    };

}

/************** Module exports ***********************/
module.exports = createRdKafkaStatsCb;
