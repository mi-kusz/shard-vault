# ShardVault

ShardVault is a distributed data storage system designed for artifact management. The system splits data into smaller chunks (shards) and replicates them across multiple warehouses to ensure reliability and fault tolerance.

This project was created as a personal learning experience to explore distributed systems, Akka, and modern testing practices.

## Features

- **Data Sharding** – Divides data into smaller chunks (shards) for better scalability and performance.
- **Sharding Replication** – Each shard is replicated across multiple warehouses to ensure fault tolerance and data availability.
- **Fault Tolerance** – Data is replicated to multiple locations for redundancy and fault tolerance.
- **Testing** – The project uses Akka TestKit and JUnit 5 for unit and integration testing.

## Used Technologies

- **Java** – The programming language used to develop the entire project.
- **Akka** – A framework for building distributed systems based on the actor model, used for managing communication between different components of the system.
- **Google Guava** – A library providing helper classes and functions for working with collections and other utilities.
- **JUnit 5** – A framework for unit testing, used to test the application's functionality.
- **Akka TestKit** – A testing tool for Akka-based systems, enabling easy testing of actors and their interactions.
