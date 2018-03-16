# Datafan

We have a mesh of Members. Each Member is connected to 1-N other Member.
A Member is **owned** of a set of data. Only that Member can modify the data that it owns.

Datafan propagates data of each Member through the mesh. This way each Member have a local copy of all other Member data.

Nothing ensure that the whole set of data is consistent over the mesh. The system however converge.

Datafan isa lightweight gossip propagation engine.

The interfaces are:

Member: Actor attached to a mesh. It must be backed by a storage that allow data sharding (per owner). It owns data, can do CRUD on its dataset and Read Only on data shard associated to other owner. Dedicated primitive are used to put/delete data coming from other Members.

Item: Represent the data exchange by the members over the mesh. An Item has is uniquely identified by a pair {Key,OwnerID}. The ownerID uniquely identifies a member of the mesh. The Key uniquely identifies a data insinde the owner datastore. An Item also hold the timestamp of its last modification.

Connector: Repesent the way to connect the different members of the mesh. The engine uses channel to communicate with the connector. The connector is in charge of transmitting the data from one Member to the others. In case a Member is represented by a process the connector is the interface to the RPC of your choice.

## Implementation:
That first implementation synchronizes the members periodically. Each member can define its own synchronization period.
A synchronization between Members happens in  one way and consists in 3 steps:
- Exchanging and index of Key and their associated time stamp
- Comparing the latest index with the recieved index and deduce the delta (key to be update)
- Send a request for the set of keys corresponding to delta
- Receive the the updated data

## Simple package
The *simple* package implements the interface to run multiple engines in the same process. The goal is to validate the engien. It is backed by an im memory storage built on top of maps. The connector simple connect the channels of the different members.

## grpc
To do: connector implementation based on grpc to exchange any data between members in different process.