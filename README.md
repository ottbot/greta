# greta

A modern and (fairly) low-level Kafka client for Clojure projects.

It implementments the API introduced in Kafka 0.8.3 as an Aleph TCP client.

Development targets Kafka 0.9.0+.


## Usage

FIXME

## Contributing

The tests assume that the `greta-tests` topic already exists, and that
topics are _not_ automatically created. This not default, so you must
add `auto.create.topics.enable = false` to your broker configuration.




## License

Copyright © 2015 Funding Circle

Distributed under the BSD 3-Clause License.
