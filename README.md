A library for go with primitives that allow you to build structures similar to
the ones provided in kafka streams.

Designing principle of this library is to produce elemental composable
primitives for constructing streams. For example, original kafka streams don't
allow us to construct streams that join streams with keys of different nature
or require too much jumping through the hoops to achieve it.

Table Base
----------

Table base is a primitive that allows you to connect to a topic and start
consuming from it. The consumer will consume from all partitions of the
provided topic. The output of your processor will be stored in a local store.
Offsets will be stored in the local store as well.

Global Table
------------

Is a simple wrapper around table base. It configures table base with a simple
processor that just passes values to the store.

Table
-----

Table is a primitive that is configured with group id. If the application
is started on several nodes then each node will handle only a subset
of partitions. Table maintains a compacted change log topic with the latest
values received in the source topic.
