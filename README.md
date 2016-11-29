# Documentation #

Utility module to flatten an object, filter keys and send wanted metrics to statsd.

This package has an implicit dependency on an statsd client that implements the function gauge.

Callers can specify the keys they are interested on sending to statsd or use
the filtered defaults which are these ones:

```
    [
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
```

If using a filter function metrics that include '-1' or 'toppars' are not sent.


## Usage ##


### Example: Filter function ####


```javascript

var StatsD  require('node-statsd'),
client  new StatsD({ host: 'statsd.eqiad.wmnet'});  // or metrics-reporter

function myFilter(key) = {...}

var rdkafkaStatsdCb = require('node-rdkafka-statsd')(client,{'filterFn': myFilter});

var kafka = require('node-rdkafka');

var consumer = new kafka.KafkaConsumer({
        ...
        'statistics.interval.ms': 30000,
});

 // Flowing mode
consumer.connect();
consumer
    .on('ready', function() {
            consumer.consume('some-topic');
        })
    .on('event.stats', rdkafkaStatsdCb);

```

#### Example: No filter function ####

When no filter function is used all metrics are sent to statsd

```javascript

var StatsD  require('node-statsd'),
client  new StatsD({ host: 'statsd.eqiad.wmnet'});  // or metrics-reporter

var rdkafkaStatsdCb = require('node-rdkafka-statsd')(client,{'filterFn': false});

```
