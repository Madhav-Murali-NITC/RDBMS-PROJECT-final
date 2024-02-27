# NITCbase: RDBMS Implementation Project

## Introduction
NITCbase is a Relational Database Management System (RDBMS) implementation project aimed at helping undergraduate students grasp the design and data structures of an elementary RDBMS by building one themselves. The project provides a step-by-step implementation roadmap, detailed documentation with tutorials, and complete design and specification of the RDBMS and its various component subsystems.

## About
NITCbase follows an eight-layer design, encompassing the basic capabilities of a standard RDBMS, including table creation and deletion, record insertion, selection queries, and indexing using B+ Trees. The implemented RDBMS supports elementary SQL queries such as create, drop, alter, insert, select, project, equi-join, and indexing queries like create index and drop index. However, concurrency support is currently not included.

## Components Overview
The eight layers of NITCbase are as follows:

1. Physical Layer
2. Buffer Layer
3. B+ Tree Layer
4. Block Access Layer
5. Cache Layer
6. Algebra Layer
7. Schema Layer
8. Frontend Interface

## Command-Line Interfaces
NITCbase provides two command-line interfaces:
- Frontend Interface
- XFS Interface

## Provided Resources
The project assumes a Unix/Linux environment and provides resources to get started, including:
- XFS Interface for disk access
- Disk class implementation
- Class definitions for intermediate layers
- Partial implementation of the Frontend Interface
- Detailed documentation with specifications, tutorials, and design details
- A roadmap for step-by-step implementation

## Functional Description of Layers
Each layer of NITCbase serves specific functionalities:
1. **Frontend User Interface Sub-layer**: Interacts with users, translating SQL-like queries into calls to appropriate functions in the Frontend Programming Interface Sub-layer.
2. **Frontend Programming Interface Sub-layer**: Redirects calls to lower layers, handling complex queries and directing DDL and DML commands to corresponding layers.
3. **Schema Layer**: Manages DDL commands for creating, deleting, and modifying relations and attributes.
4. **Algebra Layer**: Processes DML commands such as Insert, Select, Project, and Join.
5. **Cache Layer**: Implements runtime memory data structures for managing open relations' data and metadata.
6. **Block Access Layer**: Implements fundamental data manipulation operations like select and project.
7. **B+ Tree Layer**: Handles indexing operations using B+ trees, simplifying data retrieval and insertion for indexed queries.
8. **Buffer Layer**: Manages a disk buffer to cache disk blocks, optimizing disk access and providing a clean interface to higher layers.
9. **Physical Layer**: Handles low-level disk access routines, facilitating reading and writing data blocks.

## Conclusion
NITCbase offers a comprehensive learning experience for implementing an RDBMS from scratch, providing students with practical insights into database design and data structures.
