A library for go with primitives that allow you to build structures similar to
the ones provided in kafka streams.

Designing principle of this library is to produce elemental composable
primitives for constructing streams. For example, original kafka streams don't
allow us to construct streams that join streams with keys of different nature
or require too much jumping through the hoops to achieve it.
