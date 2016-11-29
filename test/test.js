"use strict";

const assert = require('assert');


describe('Testing object flattening. No filter function (Blackbox, through exposed function)', function () {

    const fakeStatsdReporter = {
        set: function () {},
        gauge: function () {}
    };
    const cb = require('../lib/rdkafka-statsd')(fakeStatsdReporter, {
        'filterFn': false
    });

    it('Testing object flattening happy case', function () {

        var a = {
            'pepe': {
                'pepe': 45,
                'ana': 23,
                'juanito': 45,
                'bad:key': 'bad!',
            },
            'ana': 63,
            'juanito': 45,
            'bad:ana': 'hola!'


        };

        var rawObject = {
            "message": JSON.stringify(a)
        };

        var flat_a = cb(rawObject);
        assert.deepEqual(flat_a.ana, 63);
        assert.deepEqual(flat_a['pepe.ana'], 23);
        assert.deepEqual(flat_a['juanito'], 45);
        //':' substitution happened
        assert.deepEqual(flat_a['bad_ana'], 'hola!');
        assert.deepEqual(flat_a['pepe.bad_key'], 'bad!');


    });

});

describe('Testing object flattening and filtering with passed-in filter function (Blackbox, through exposed function)', function () {

    const fakeFilterFn = function (key) {
        var l = ['ana', 'simple_cnt', 'rx_ver_drops', 'lo_offset'];
        return l.indexOf(key) >= 0;

    };

    const fakeStatsdReporter = {
        gauge: function () {}
    };
    const cb = require('../lib/rdkafka-statsd')(fakeStatsdReporter, {
        'filterFn': fakeFilterFn
    });

    it('Testing object flattening happy case', function () {

        var a = {
            'pepe': {
                'pepe': 45,
                'ana': 23,
                'juanito': 45
            },
            'ana': 63,
            'juanito': 45
        };


        var rawObject = {
            "message": JSON.stringify(a)
        };

        var flat_a = cb(rawObject);
        assert.deepEqual(flat_a.ana, 63);
        assert.deepEqual(flat_a['pepe.ana'], 23);
        assert(flat_a.juanito === undefined);

    });

    it('Testing object flattening empty object', function () {

        var a = {};
        var flat_a = cb(a);
        assert.deepEqual(flat_a, {});

    });

    it('Testing object flattening no object', function () {

        var a = "hola";
        var flat_a = cb(a);
        assert.deepEqual(flat_a, {});

    });


    it('Testing object flattening null object', function () {

        var a = null;
        var flat_a = cb(a);
        assert.deepEqual(flat_a, {});

    });


    it('Test flattening, real kafka object', function () {
        var metrics = {
            "topics": {
                "test4": {
                    "partitions": {
                        "-1": {
                            "rx_ver_drops": 0,
                            "msgs": 0,
                            "txbytes": 0,
                            "txmsgs": 0,
                            "consumer_lag": -1,
                            "hi_offset": -1001,
                            "lo_offset": -1001,
                            "eof_offset": -1001,
                            "committed_offset": -1001,
                            "xmit_msgq_bytes": 0,
                            "xmit_msgq_cnt": 0,
                            "msgq_bytes": 0,
                            "msgq_cnt": 0,
                            "unknown": false,
                            "desired": false,
                            "leader": -1,
                            "partition": -1,
                            "fetchq_cnt": 0,
                            "fetchq_size": 0,
                            "fetch_state": "none",
                            "query_offset": 0,
                            "next_offset": 0,
                            "app_offset": -1001,
                            "stored_offset": -1001,
                            "commited_offset": -1001
                        },
                        "0": {
                            "rx_ver_drops": 0,
                            "msgs": 0,
                            "txbytes": 0,
                            "txmsgs": 0,
                            "consumer_lag": 0,
                            "hi_offset": 34784,
                            "lo_offset": -1001,
                            "eof_offset": 34784,
                            "committed_offset": -1001,
                            "xmit_msgq_bytes": 0,
                            "xmit_msgq_cnt": 0,
                            "msgq_bytes": 0,
                            "msgq_cnt": 0,
                            "unknown": false,
                            "desired": true,
                            "leader": 0,
                            "partition": 0,
                            "fetchq_cnt": 30582,
                            "fetchq_size": 6224678,
                            "fetch_state": "active",
                            "query_offset": 0,
                            "next_offset": 34784,
                            "app_offset": 6143,
                            "stored_offset": 6143,
                            "commited_offset": -1001
                        }
                    },
                    "metadata_age": 5995,
                    "topic": "test4"
                }
            },


            "brokers": {
                "mediawiki-vagrant.dev:9092/0": {
                    "toppars": {
                        "test4": {
                            "partition": 0,
                            "topic": "test4"
                        }
                    },
                    "throttle": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rtt": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rxpartial": 3,
                    "rxcorriderrs": 0,
                    "waitresp_msg_cnt": 0,
                    "waitresp_cnt": 1,
                    "outbuf_msg_cnt": 0,
                    "outbuf_cnt": 0,
                    "stateage": 7023265,
                    "state": "UP",
                    "nodeid": 0,
                    "name": "mediawiki-vagrant.dev:9092/0",
                    "tx": 57,
                    "txbytes": 3609,
                    "txerrs": 0,
                    "txretries": 0,
                    "req_timeouts": 0,
                    "rx": 56,
                    "rxbytes": 3406226,
                    "rxerrs": 0
                },
                "localhost:9092/bootstrap": {
                    "toppars": {},
                    "throttle": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rtt": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rxpartial": 0,
                    "rxcorriderrs": 0,
                    "waitresp_msg_cnt": 0,
                    "waitresp_cnt": 0,
                    "outbuf_msg_cnt": 0,
                    "outbuf_cnt": 0,
                    "stateage": 7058566,
                    "state": "UP",
                    "nodeid": -1,
                    "name": "localhost:9092/bootstrap",
                    "tx": 5,
                    "txbytes": 172,
                    "txerrs": 0,
                    "txretries": 0,
                    "req_timeouts": 0,
                    "rx": 5,
                    "rxbytes": 8146,
                    "rxerrs": 0
                }
            },

            "name": "rdkafka#consumer-1",
            "type": "consumer",
            "ts": 69158506047,
            "time": 1472147268,
            "replyq": 30583,
            "msg_cnt": 0,
            "msg_max": 100000,
            "simple_cnt": 0
        };


        var rawObject = {
            "message": JSON.stringify(metrics)
        };

        var flat_metrics = cb(rawObject);
        assert.deepEqual(flat_metrics['simple_cnt'], 0);
        assert.deepEqual(flat_metrics['topics.test4.partitions.0.rx_ver_drops'], 0);
        // '-1' is filtered
        assert.deepEqual(flat_metrics['topics.test4.partitions.-1.lo_offset'], undefined);
        assert.deepEqual(flat_metrics['topics.test4.partitions.0.lo_offset'], -1001);

        assert(flat_metrics.msg_max === undefined);

    });

});


describe('Testing object flattening and filtering with default filter function (Blackbox, through exposed function)', function () {

    const fakeStatsdReporter = {
        gauge: function () {}
    };

    const cb = require('../lib/rdkafka-statsd')(fakeStatsdReporter);


    it('Test flattening and filtering default function, real kafka object', function () {
        var metrics = {
            "topics": {
                "test4": {
                    "partitions": {
                        "-1": {
                            "rx_ver_drops": 0,
                            "msgs": 0,
                            "txbytes": 0,
                            "txmsgs": 0,
                            "consumer_lag": -1,
                            "hi_offset": -1001,
                            "lo_offset": -1001,
                            "eof_offset": -1001,
                            "committed_offset": -1001,
                            "xmit_msgq_bytes": 0,
                            "xmit_msgq_cnt": 0,
                            "msgq_bytes": 0,
                            "msgq_cnt": 0,
                            "unknown": false,
                            "desired": false,
                            "leader": -1,
                            "partition": -1,
                            "fetchq_cnt": 0,
                            "fetchq_size": 0,
                            "fetch_state": "none",
                            "query_offset": 0,
                            "next_offset": 0,
                            "app_offset": -1001,
                            "stored_offset": -1001,
                            "commited_offset": -1001
                        },
                        "0": {
                            "rx_ver_drops": 0,
                            "msgs": 0,
                            "txbytes": 0,
                            "txmsgs": 0,
                            "consumer_lag": 0,
                            "hi_offset": 34784,
                            "lo_offset": -1001,
                            "eof_offset": 34784,
                            "committed_offset": -1001,
                            "xmit_msgq_bytes": 0,
                            "xmit_msgq_cnt": 0,
                            "msgq_bytes": 0,
                            "msgq_cnt": 0,
                            "unknown": false,
                            "desired": true,
                            "leader": 0,
                            "partition": 0,
                            "fetchq_cnt": 30582,
                            "fetchq_size": 6224678,
                            "fetch_state": "active",
                            "query_offset": 0,
                            "next_offset": 34784,
                            "app_offset": 6143,
                            "stored_offset": 6143,
                            "commited_offset": -1001
                        }
                    },
                    "metadata_age": 5995,
                    "topic": "test4"
                }
            },


            "brokers": {
                "mediawiki-vagrant.dev:9092/0": {
                    "toppars": {
                        "test4": {
                            "partition": 0,
                            "topic": "test4"
                        }
                    },
                    "throttle": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rtt": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rxpartial": 3,
                    "rxcorriderrs": 0,
                    "waitresp_msg_cnt": 0,
                    "waitresp_cnt": 1,
                    "outbuf_msg_cnt": 0,
                    "outbuf_cnt": 0,
                    "stateage": 7023265,
                    "state": "UP",
                    "nodeid": 0,
                    "name": "mediawiki-vagrant.dev:9092/0",
                    "tx": 57,
                    "txbytes": 3609,
                    "txerrs": 0,
                    "txretries": 0,
                    "req_timeouts": 0,
                    "rx": 56,
                    "rxbytes": 3406226,
                    "rxerrs": 0
                },
                "localhost:9092/bootstrap": {
                    "toppars": {},
                    "throttle": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rtt": {
                        "cnt": 0,
                        "sum": 0,
                        "avg": 0,
                        "max": 0,
                        "min": 0
                    },
                    "rxpartial": 0,
                    "rxcorriderrs": 0,
                    "waitresp_msg_cnt": 0,
                    "waitresp_cnt": 0,
                    "outbuf_msg_cnt": 0,
                    "outbuf_cnt": 0,
                    "stateage": 7058566,
                    "state": "UP",
                    "nodeid": -1,
                    "name": "localhost:9092/bootstrap",
                    "tx": 5,
                    "txbytes": 172,
                    "txerrs": 0,
                    "txretries": 0,
                    "req_timeouts": 0,
                    "rx": 5,
                    "rxbytes": 8146,
                    "rxerrs": 0
                }
            },

            "name": "rdkafka#consumer-1",
            "type": "consumer",
            "ts": 69158506047,
            "time": 1472147268,
            "replyq": 30583,
            "msg_cnt": 0,
            "msg_max": 100000,
            "simple_cnt": 0
        };

        var rawObject = {
            "message": JSON.stringify(metrics)
        };

        var flat_metrics = cb(rawObject);


        assert.deepEqual(flat_metrics['simple_cnt'], undefined);
        assert.deepEqual(flat_metrics['topics.test4.partitions.0.leader'], undefined);
        assert.deepEqual(flat_metrics['topics.test4.partitions.-1.lo_offset'], undefined);
        assert.deepEqual(flat_metrics['topics.test4.partitions.0.stored_offset'], 6143);

    });

});
describe('Testing object reporting', function () {

    it('Testing object reporting happy case', function (done) {
        // given that code execution is synchronous these assertions are guranteed
        // to run
        const fakeStatsdReporter = {
            gauge: function (item, itemData) {
                assert.equal(itemData, 1001);
            }
        };
        const cb = require('../lib/rdkafka-statsd')(fakeStatsdReporter, {
            'filterFn': false
        });

        var a = {
            "topics": {
                "rx_ver_drops": [],
                "msgs": 1001,
                "txmsgs": "this is a string"

            }
        };

        var rawObject = {
            "message": JSON.stringify(a)
        };
        var flat_a = cb(rawObject);
        done();
    });

    it('Testing object reporting bad metrics, should not report', function (done) {
        // given that code execution is synchronous these assertions are guranteed
        // to run if the reporter is called
        // it should not be as the only metric we have is neither a
        // string nor a number
        const fakeStatsdReporter = {

            gauge: function (item, itemData) {
                assert.equal(true, false);
            }
        };
        const cb = require('../lib/rdkafka-statsd')(fakeStatsdReporter, {
            'filterFn': false
        });

        var a = {
            "topics": {
                "rx_ver_drops": [],

            }
        };
        var rawObject = {
            "message": JSON.stringify(a)
        };
        var flat_a = cb(rawObject);
        done();
    });

    it('Testing object reporting, removing chars ', function (done) {
        // given that code execution is synchronous these assertions are guranteed

        const fakeStatsdReporter = {

            gauge: function (item, itemData) {
                assert.equal(item, 'topics_rx_ver_drops');
            }
        };
        const cb = require('../lib/rdkafka-statsd')(fakeStatsdReporter, {
            'filterFn': false
        });

        var a = {
            "topics": {
                "rx.ver/drops": 43,

            }
        };
        var rawObject = {
            "message": JSON.stringify(a)
        };
        var flat_a = cb(rawObject);
        done();
    });

});
