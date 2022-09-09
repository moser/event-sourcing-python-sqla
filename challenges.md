# Challenges in event sourcing

## Choosing an event store

### Specialized DB

Options:

* [Eventstore DB](https://www.eventstore.com/)
  * - No public cloud offerings
  * - No good python bindings (only community maintained)
  * - pretty slow in a small test (could be the bad bindings though)
  * + From the guy that came up with CQRS
  * - Community uses mostly .NET
* [Kafka](https://kafka.apache.org/) 
  * + Both AWS & Azure have a cloud offering
  * - No prior knowledge
  * - Hard to change partioning
* Cloud-provider specific offerings
  * - Lock-in
  * - Small community

### Relational DB

#### Problem: Global, gapless ordering of events is hard

Attaining a global, gapless order of events (which would be optimal for
consumers to keep track of which events they have already processed) is hard.
It is possible by aggressively locking a shared resource, but that effectively
reduces the concurrency to 1.

An alternative is accepting gaps (stemming from cancelled or failed transactions)
and certain limits to the strictness of the order. By a relaxed strictness, I
mean accepting that for some events `t_event1 < t_event2` but `o_event1 >
o_event2` where `t` is the real time the event was triggered by the system and
`o` is the order). Read more on this approach
[here](https://softwaremill.com/implementing-event-sourcing-using-a-relational-database/#cross-stream-type-projections).

On the consumer side, gaps are OK as long as 
- Event producer transactions are short
- Consumers (deliberately) lag a bit behind the producers (more than the typical/max producer transaction length)

One maybe wants to add some way of monitoring for gaps:
- counting events and comparing with producer side
- taking note of the last X events and regularly comparing the list with a fresh copy of the last X events

Gaps will be seldom. Consumers can also back-off a bit if they see a gap to give
a slow transaction time to finish. (Increases consumer lag in those cases though)


## CQRS is probably key for simplicity

Without [CQRS](https://martinfowler.com/bliki/CQRS.html) the model/persistence/repos
will probably become entangled with the event-based write concern and the more
object/table-oriented read concern.

I see major advantages in this approach over a single model for read/write:
* multiple views of same data possible
* cached data for (otherwise) complex queries
* possibility to even use a separate database for the read models
* filling alternative storages, e.g. a search engine like ElasticSearch, becomes a no-brainer with this approach


## Eventual consistency

Having to use CQRS, we will have to deal with eventual consistency (because it
is to costly to update the read models within the DB transaction for events
which we want to be as short as possible for reasons detailed in the section
about the global order).

This can be solved by making the API clients (frontend etc) aware of the fact
that they are dealing with such a system. Mutations could pass back the global version number
of the (last) event they generated. An API client can then ask the query interface
to return an object at this version (if the version is not yet ingested by the
underlying projection, a "retry soon" answer will be supplied).


## Versioning

Versioning of events seems to be a problem: Read more on this in [Greg Young's
book on the topic](https://leanpub.com/esversioning/read).
