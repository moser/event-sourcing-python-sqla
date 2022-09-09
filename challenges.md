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
`o` is the order).

On the consumer side, gaps are OK as long as 
- event producer transactions are short
- consumers (deliberately) lag a bit behind the producers (more than the typical/max producer transaction length)

One maybe wants to add some way of monitoring for gaps:
- counting events and comparing with producer side
- taking note of the last X events and regularly comparing the list with a fresh copy of the last X events




## Eventual consistency


## CQRS is probably key for simplicity


## Versioning
