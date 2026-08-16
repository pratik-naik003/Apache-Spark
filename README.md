# Apache Spark — Module 01: Introduction to Apache Spark

![Apache Spark Logo](https://spark.apache.org/images/spark-logo.png)

> **Apache Spark Learning Roadmap — Module 01 of 10**
>
> This module introduces Apache Spark, distributed computing, Spark's ecosystem, PySpark, and the core ideas behind Spark.

---

## 📚 Table of Contents

- [1. What is Apache Spark?](#1-what-is-apache-spark)
- [2. Why Do We Need Spark?](#2-why-do-we-need-spark)
- [3. Distributed Computing](#3-distributed-computing)
- [4. Cluster and Nodes](#4-cluster-and-nodes)
- [5. Parallel Processing](#5-parallel-processing)
- [6. Why is Spark Fast?](#6-why-is-spark-fast)
- [7. Apache Spark Ecosystem](#7-apache-spark-ecosystem)
- [8. Spark Core](#8-spark-core)
- [9. Spark SQL](#9-spark-sql)
- [10. Structured Streaming](#10-structured-streaming)
- [11. MLlib](#11-mllib)
- [12. GraphX](#12-graphx)
- [13. Apache Spark vs PySpark](#13-apache-spark-vs-pyspark)
- [14. Spark vs Pandas](#14-spark-vs-pandas)
- [15. Batch vs Streaming](#15-batch-vs-streaming)
- [16. Basic PySpark Example](#16-basic-pyspark-example)
- [17. Important Terminology](#17-important-terminology)
- [18. Key Takeaways](#18-key-takeaways)
- [19. Module 01 Checklist](#19-module-01-checklist)
- [20. References](#20-references)

---

# 1. What is Apache Spark?

**Apache Spark** is an open-source distributed computing engine designed for large-scale data processing.

Instead of processing a huge dataset on a single machine, Spark can divide the data and computation across multiple machines in a **cluster**.

### Core idea

```text
                    Large Dataset
                         |
                    Apache Spark
                         |
          --------------------------------
          |              |               |
       Machine 1      Machine 2       Machine 3
          |              |               |
          -------- Parallel Processing ---
                         |
                       Result
```

The fundamental idea is:

> **Break a large computation into smaller tasks and execute them in parallel across a cluster.**

Apache Spark can be used for:

- Large-scale batch processing
- SQL analytics
- Data transformation
- Streaming data processing
- Machine learning
- Graph processing

---

# 2. Why Do We Need Spark?

Imagine a company has **10 TB of customer data**.

Trying to process everything on one computer can be difficult because the machine has limited:

- RAM
- CPU
- Storage
- Network bandwidth

A single-machine approach looks like:

```text
10 TB Data
    |
    v
Single Computer
    |
    v
Limited Resources
    |
    v
Processing
```

Spark allows the workload to be distributed:

```text
                       10 TB Data
                           |
                           v
                    Apache Spark
                           |
          ---------------------------------
          |               |               |
       Machine 1       Machine 2       Machine 3
        ~3.3 TB         ~3.3 TB         ~3.4 TB
          |               |               |
          ---------------+----------------
                          |
                        Result
```

This allows the workload to **scale horizontally** by using additional machines.

---

# 3. Distributed Computing

## What does "distributed" mean?

**Distributed computing** means dividing a computational workload across multiple machines that work together.

Instead of:

```text
One Machine
     |
     v
Process Everything
```

we can have:

```text
                  Large Workload
                       |
        --------------------------------
        |              |               |
     Machine 1      Machine 2       Machine 3
        |              |               |
        ----------- Partial Results -----
                       |
                       v
                  Final Result
```

### Why is this useful?

Distributed computing allows us to:

- Process datasets that are too large for one machine
- Use multiple CPUs and machines
- Perform computations in parallel
- Scale workloads horizontally
- Improve throughput

---

# 4. Cluster and Nodes

A **cluster** is a collection of machines that work together.

Each machine in a cluster is commonly called a **node**.

```text
                    Cluster
                       |
        --------------------------------
        |              |               |
      Node 1         Node 2          Node 3
        |              |               |
      CPU/RAM        CPU/RAM         CPU/RAM
```

Spark can distribute computation across these nodes.

> The detailed roles of the **Driver, Executors, Worker Nodes, and Cluster Manager** are covered in **Module 02 — Spark Architecture & Execution Model**.

---

# 5. Parallel Processing

**Parallel processing** means performing multiple computations at the same time.

Suppose we have:

```text
1,000,000 records
```

Instead of processing all records sequentially on one machine:

```text
Machine
   |
   v
1,000,000 Records
```

Spark can divide the workload:

```text
Machine 1 -> 250,000 records
Machine 2 -> 250,000 records
Machine 3 -> 250,000 records
Machine 4 -> 250,000 records
```

The different portions can be processed concurrently.

### Important relationship

```text
Large Dataset
     |
     v
Partition into smaller pieces
     |
     v
Parallel Processing
     |
     v
Combine / Produce Result
```

The actual execution details depend on Spark's execution plan, partitions, resources, and workload.

---

# 6. Why is Spark Fast?

Spark's performance comes from multiple design features. It is **not simply because Spark uses RAM**.

## 6.1 Parallel Processing

Multiple workers can process different portions of a dataset concurrently.

---

## 6.2 In-Memory Computation

Spark can keep data in memory when appropriate.

This can reduce repeated disk I/O when the same data is reused.

For example:

```text
Traditional repeated processing:

Data -> Disk -> Process
Data -> Disk -> Process
Data -> Disk -> Process
```

With suitable Spark caching:

```text
Data
 |
 v
Memory
 |
 +----> Operation 1
 |
 +----> Operation 2
 |
 +----> Operation 3
```

> **Important:** Spark is not an "in-memory-only" system. It can use memory and disk depending on the workload and configuration.

---

## 6.3 DAG-Based Execution

Spark represents many computations as a **Directed Acyclic Graph (DAG)**.

A simplified example:

```text
Read Data
    |
    v
Filter
    |
    v
Transform
    |
    v
Group
    |
    v
Result
```

This allows Spark to reason about the computation before executing it.

We will study:

- DAG
- Jobs
- Stages
- Tasks

in later modules.

---

## 6.4 Query Optimization

Spark SQL and DataFrame workloads can be optimized by Spark's SQL execution engine.

This can include:

- Query planning
- Logical optimization
- Physical planning
- Runtime optimization

We will study optimization in **Module 08 — Spark Performance & Optimization**.

---

## 6.5 Data Partitioning

Spark divides distributed data into **partitions**.

```text
             Large Dataset
                   |
       -------------------------
       |       |       |       |
      P1      P2      P3      P4
```

Partitions provide units of parallel work.

We will study partitions, shuffle, `repartition()`, and `coalesce()` in **Module 07**.

---

# 7. Apache Spark Ecosystem

Apache Spark provides a unified platform for several kinds of data workloads.

```text
                         Apache Spark
                              |
        ------------------------------------------------
        |               |              |              |
    Spark Core       Spark SQL    Structured      MLlib
                                  Streaming
        |
      GraphX
```

The major components you should know are:

| Component | Main Purpose |
|---|---|
| **Spark Core** | Fundamental distributed execution engine |
| **Spark SQL** | Structured data processing and SQL |
| **Structured Streaming** | Streaming data processing |
| **MLlib** | Distributed machine learning |
| **GraphX** | Graph processing |

---

# 8. Spark Core

**Spark Core** provides the fundamental execution capabilities of Apache Spark.

It is responsible for core concepts such as:

- Distributed task execution
- Task scheduling
- Fault tolerance
- RDDs
- Memory management
- Basic distributed computation

You can think of Spark Core as the **foundation** on which higher-level Spark APIs are built.

```text
              Spark SQL
                  |
        Structured Streaming
                  |
                MLlib
                  |
                GraphX
                  |
             Spark Core
```

---

# 9. Spark SQL

**Spark SQL** is Spark's module for working with structured data.

It provides:

- SQL queries
- DataFrames
- Structured data processing
- Query optimization
- Access to multiple data sources

Example:

```sql
SELECT department, AVG(salary)
FROM employees
GROUP BY department;
```

You can also perform similar operations using the DataFrame API.

```python
result = (
    employees
    .groupBy("department")
    .avg("salary")
)
```

Both SQL and DataFrame operations use Spark's underlying execution engine.

---

# 10. Structured Streaming

**Structured Streaming** is Spark's API for processing continuously arriving data using the structured DataFrame/SQL model.

A common architecture looks like:

```text
Real-Time Events
       |
       v
     Kafka
       |
       v
Structured Streaming
       |
       v
Transform / Aggregate
       |
       v
Database / Data Lake / Dashboard
```

Example use cases:

- Real-time analytics
- Log processing
- IoT data processing
- Fraud detection pipelines
- Monitoring systems

> Kafka and Spark are different technologies. Kafka is commonly used as a streaming source, while Spark can process the incoming stream.

Structured Streaming will be covered in **Module 10**.

---

# 11. MLlib

**MLlib** is Apache Spark's machine learning library.

It provides tools for scalable machine learning workflows.

Examples include:

- Classification
- Regression
- Clustering
- Feature engineering
- Model evaluation
- ML pipelines

A simplified workflow:

```text
Large Dataset
      |
      v
Spark DataFrame
      |
      v
Feature Engineering
      |
      v
MLlib
      |
      v
Machine Learning Model
      |
      v
Predictions
```

For an AI/ML student, MLlib is useful when machine learning needs to be performed over large datasets.

---

# 12. GraphX

**GraphX** is Spark's graph processing API.

Graphs represent entities and relationships.

Example:

```text
        User A
        /    \
       /      \
   User B ---- User C
```

Potential applications include:

- Social network analysis
- Relationship analysis
- Network analysis
- Graph algorithms

GraphX is part of Spark's ecosystem, although most modern Spark learning paths focus more heavily on Spark SQL, DataFrames, and Structured Streaming.

---

# 13. Apache Spark vs PySpark

These two terms are related but not identical.

### Apache Spark

Apache Spark is the distributed computing engine.

### PySpark

**PySpark is the Python API for Apache Spark.**

This allows Python developers to use Spark.

```text
                 Apache Spark
                      |
       --------------------------------
       |              |               |
     Scala          Java           Python
                                      |
                                   PySpark
```

If you are learning Spark using Python, most of your code will be written using **PySpark**.

---

# 14. Spark vs Pandas

Both Spark and pandas provide DataFrame-style APIs, but they are designed for different execution models.

| Feature | Pandas | Apache Spark |
|---|---|---|
| Primary focus | Local data analysis | Distributed data processing |
| Execution | Usually single machine | Distributed across a cluster |
| Scaling | Mostly vertical | Horizontal + distributed |
| DataFrame | Yes | Yes |
| SQL engine | No native distributed SQL engine | Spark SQL |
| Distributed processing | No | Yes |
| Large-scale processing | Limited by local resources | Designed for large-scale workloads |
| Python support | Native | PySpark |
| Typical use | EDA, data analysis | Big data, ETL, analytics, streaming |

### Important

Do not memorize:

> `pandas = small data` and `Spark = big data`

That is too simplistic.

A better mental model is:

> **pandas is primarily a local DataFrame/data-analysis library, while Spark is designed as a distributed data processing engine.**

---

# 15. Batch vs Streaming

Spark supports both batch and streaming workloads.

## Batch Processing

Data is collected and processed as a bounded dataset.

```text
Data
 |
 v
Spark
 |
 v
Process Entire Dataset
 |
 v
Result
```

Example:

> Calculate the total sales for yesterday.

---

## Streaming Processing

Data continuously arrives and is processed as it becomes available.

```text
Event 1 ----\
Event 2 -----\
Event 3 ------> Spark Streaming -> Results
Event 4 -----/
Event 5 ----/
```

Example:

> Calculate the number of orders arriving every minute.

---

## Batch vs Streaming

| Feature | Batch | Streaming |
|---|---|---|
| Data | Bounded | Continuously arriving |
| Processing | Periodic | Continuous / incremental |
| Example | Daily sales report | Live sales dashboard |
| Spark API | Spark SQL / DataFrame APIs | Structured Streaming |

---

# 16. Basic PySpark Example

PySpark can be installed with:

```bash
pip install pyspark
```

## Create a SparkSession

`SparkSession` is the main entry point for modern Spark functionality in PySpark.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MyFirstSparkApp")
    .getOrCreate()
)

print("Spark started!")
```

---

## Create a DataFrame

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FirstSparkApp")
    .getOrCreate()
)

data = [
    ("Rahul", 20),
    ("Amit", 22),
    ("Raj", 21)
]

columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

df.show()
```

### Output

```text
+-----+---+
| name|age|
+-----+---+
|Rahul| 20|
| Amit| 22|
|  Raj| 21|
+-----+---+
```

At this point, the goal is only to understand that PySpark lets us create and work with Spark DataFrames.

We will study DataFrames properly in **Module 04**.

---

# 17. Important Terminology

| Term | Meaning |
|---|---|
| **Apache Spark** | Distributed computing engine |
| **PySpark** | Python API for Apache Spark |
| **Cluster** | Group of machines working together |
| **Node** | Individual machine in a cluster |
| **Distributed Computing** | Computing performed across multiple machines |
| **Parallel Processing** | Multiple computations performed concurrently |
| **Partition** | A logical chunk of distributed data |
| **Spark Core** | Foundation of Spark's execution engine |
| **Spark SQL** | Structured data and SQL processing |
| **Structured Streaming** | Streaming data processing API |
| **MLlib** | Spark's machine learning library |
| **GraphX** | Spark's graph processing API |
| **DAG** | Directed Acyclic Graph representing computation |
| **PySpark** | Python interface to Spark |

---

# 18. Key Takeaways

### 1. Spark is a distributed computing engine

It is designed to process large-scale data across multiple machines.

### 2. Spark uses parallel processing

Different portions of data can be processed concurrently.

### 3. Spark can scale horizontally

Instead of relying only on a bigger machine, a workload can use more machines in a cluster.

### 4. Spark is not just an in-memory system

It can use memory and disk depending on the workload and configuration.

### 5. Spark has multiple components

The major ones are:

```text
Spark Core
Spark SQL
Structured Streaming
MLlib
GraphX
```

### 6. PySpark is the Python API

If you use Python to work with Spark, you will generally use PySpark.

### 7. DataFrames and Spark SQL are extremely important

Modern Spark development heavily uses structured APIs such as DataFrames and SQL.

---

# 19. Module 01 Checklist

- [ ] Understand what Apache Spark is
- [ ] Understand why Spark is needed
- [ ] Understand distributed computing
- [ ] Understand clusters and nodes
- [ ] Understand parallel processing
- [ ] Understand Spark's basic performance ideas
- [ ] Know Spark Core
- [ ] Know Spark SQL
- [ ] Know Structured Streaming
- [ ] Know MLlib
- [ ] Know GraphX
- [ ] Understand Apache Spark vs PySpark
- [ ] Understand Spark vs pandas
- [ ] Understand batch vs streaming
- [ ] Run a basic PySpark program
- [ ] Understand what a Spark DataFrame is at a high level

---

# 20. References

- [Apache Spark Official Website](https://spark.apache.org/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL & DataFrames](https://spark.apache.org/sql/)
- [Apache Spark History](https://spark.apache.org/history.html)

---
![Description of image](image1.png)


# Apache Spark — Module 02: Spark Architecture & Execution Model

![Apache Spark](https://spark.apache.org/images/spark-logo.png)

> **Apache Spark Learning Roadmap — Module 02 of 10**
>
> This module explains how a Spark application actually runs: **Application → Driver → Cluster Manager → Worker Nodes → Executors → Tasks**.

---

## 📚 Table of Contents

- [1. Big Picture](#1-big-picture)
- [2. Spark Application](#2-spark-application)
- [3. Driver Program](#3-driver-program)
- [4. Cluster Manager](#4-cluster-manager)
- [5. Worker Node](#5-worker-node)
- [6. Executor](#6-executor)
- [7. Task](#7-task)
- [8. Complete Spark Architecture](#8-complete-spark-architecture)
- [9. How a Spark Application Runs](#9-how-a-spark-application-runs)
- [10. Driver vs Executor](#10-driver-vs-executor)
- [11. Worker Node vs Executor](#11-worker-node-vs-executor)
- [12. Cluster Manager vs Worker Node](#12-cluster-manager-vs-worker-node)
- [13. SparkSession](#13-sparksession)
- [14. Job, Stage and Task — Preview](#14-job-stage-and-task--preview)
- [15. Local Mode vs Cluster Mode](#15-local-mode-vs-cluster-mode)
- [16. PySpark Example](#16-pyspark-example)
- [17. Spark Web UI](#17-spark-web-ui)
- [18. Important Terminology](#18-important-terminology)
- [19. Key Takeaways](#19-key-takeaways)
- [20. Module 02 Checklist](#20-module-02-checklist)
- [21. References](#21-references)

---

# 1. Big Picture

When you run a Spark application, several components work together.

The simplified architecture is:

```text
                         SPARK APPLICATION
                                |
                                v
                         DRIVER PROGRAM
                                |
                                v
                         CLUSTER MANAGER
                                |
              +-----------------+-----------------+
              |                 |                 |
              v                 v                 v
         WORKER NODE       WORKER NODE       WORKER NODE
              |                 |                 |
          EXECUTOR          EXECUTOR          EXECUTOR
              |                 |                 |
          +---+---+         +---+---+         +---+---+
          |   |   |         |   |   |         |   |   |
         T1  T2  T3        T4  T5  T6        T7  T8  T9
```

### The easiest way to remember it

```text
Application
     ↓
Driver
     ↓
Cluster Manager
     ↓
Worker Nodes
     ↓
Executors
     ↓
Tasks
```

Apache Spark's official cluster overview describes Spark applications as sets of processes coordinated by the driver, with cluster managers allocating resources and executors running computations on worker nodes.  
[Apache Spark — Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)

---

# 2. Spark Application

A **Spark Application** is a user program built using Spark.

For example:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SalesAnalysis")
    .getOrCreate()
)

df = spark.read.csv("sales.csv", header=True)

df.show()
```

This entire program represents a **Spark application**.

According to the Spark documentation, an application consists of a **driver program** and **executors** running on the cluster.

---

# 3. Driver Program

The **Driver Program** is the main coordinating process of a Spark application.

### Think of it as:

> 🧠 **Driver = Brain / Manager**

The driver:

- Runs the main application program
- Creates the Spark execution context
- Coordinates the application
- Creates execution plans
- Schedules tasks
- Tracks task/executor status
- Communicates with executors

Simplified:

```text
                  DRIVER
                    |
       +------------+------------+
       |            |            |
    Schedule     Coordinate    Track
      Tasks        Work        Status
```

### Important

The driver does **not** normally perform all distributed data processing itself.

Instead, it coordinates executors that perform the actual tasks.

---

# 4. Cluster Manager

A **Cluster Manager** is responsible for allocating resources for Spark applications.

Think of it as:

> 🧑‍💼 **Cluster Manager = Resource Manager**

The driver requests resources, and the cluster manager helps provide the resources needed to run executors.

Spark supports multiple cluster managers, including:

- **Standalone**
- **Hadoop YARN**
- **Kubernetes**

```text
                    DRIVER
                      |
                      v
               CLUSTER MANAGER
                      |
          +-----------+-----------+
          |           |           |
          v           v           v
       Worker      Worker      Worker
        Node        Node        Node
```

### Important distinction

The cluster manager manages/allocates resources.

It is **not the same thing as a worker node**.

---

# 5. Worker Node

A **Worker Node** is a machine in the cluster that can run application code.

Think of:

> 🖥️ **Worker Node = Machine providing resources**

A worker node provides resources such as:

- CPU
- Memory
- Network
- Storage

A worker node can host executor processes.

```text
              WORKER NODE
          +-------------------+
          |       CPU         |
          |       RAM         |
          |      Storage      |
          |                   |
          |     EXECUTOR      |
          +-------------------+
```

### Important

**Worker Node = Machine**

**Executor = Process running on that machine**

---

# 6. Executor

An **Executor** is a process launched for a Spark application on a worker node.

Think of:

> 💪 **Executor = Worker that performs computation**

Executors:

- Run Spark tasks
- Return task status/results to the driver
- Store data for the application when caching/persistence is used
- Use CPU and memory allocated to them

```text
                 WORKER NODE
                      |
                 +----------+
                 | EXECUTOR |
                 +----------+
                  /    |    \
                 /     |     \
              Task   Task   Task
```

Each Spark application gets its own executor processes.

This application-level isolation is an important part of Spark's architecture.

---

# 7. Task

A **Task** is a unit of work that is sent to an executor.

A simplified relationship is:

```text
Dataset
   |
   v
Partitions
   |
   v
Tasks
   |
   v
Executors
```

For example:

```text
Large Dataset
     |
     +------ Partition 1 → Task 1
     |
     +------ Partition 2 → Task 2
     |
     +------ Partition 3 → Task 3
     |
     +------ Partition 4 → Task 4
```

Executors execute these tasks.

> **Task = unit of work**

The detailed relationship between **partitions, tasks, stages, and jobs** will be covered later in the roadmap.

---

# 8. Complete Spark Architecture

## Official Apache Spark Architecture Image

The Apache Spark documentation provides an official cluster architecture diagram:

![Spark Cluster Overview](https://spark.apache.org/docs/latest/img/cluster-overview.png)

**Source:** [Apache Spark Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)

The official diagram shows the relationship between:

- Driver Program
- Cluster Manager
- Worker Nodes
- Executors
- Tasks

---

## Simplified Architecture

```text
                         +------------------+
                         | Spark Application|
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         |      Driver      |
                         |   "Coordinator"  |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         | Cluster Manager  |
                         | "Resources"      |
                         +--------+---------+
                                  |
              +-------------------+-------------------+
              |                   |                   |
              v                   v                   v
       +-------------+     +-------------+     +-------------+
       | Worker Node |     | Worker Node |     | Worker Node |
       +------+------+     +------+------+     +------+------+
              |                   |                   |
          Executor            Executor            Executor
              |                   |                   |
          +---+---+           +---+---+           +---+---+
          |   |   |           |   |   |           |   |   |
         T1  T2  T3          T4  T5  T6          T7  T8  T9
```

---

# 9. How a Spark Application Runs

Let's understand the complete flow.

Suppose we write:

```python
df = spark.read.csv("sales.csv", header=True)

result = df.filter(df.amount > 1000)

result.show()
```

What happens?

---

## Step 1 — Application starts

Your Python program starts.

```text
Python Program
      |
      v
Spark Application
```

---

## Step 2 — Driver starts

The driver becomes the coordinator of the application.

```text
Spark Application
       |
       v
     Driver
```

---

## Step 3 — Driver connects to a cluster manager

The driver requests resources needed for the application.

```text
Driver
  |
  v
Cluster Manager
```

---

## Step 4 — Executors are launched

The cluster manager allocates resources, and executors are launched on worker nodes.

```text
Worker 1 → Executor 1
Worker 2 → Executor 2
Worker 3 → Executor 3
```

---

## Step 5 — Spark creates execution work

Spark determines the work that needs to be performed.

At a high level:

```text
Read Data
   ↓
Filter
   ↓
Show Result
```

The detailed execution planning process will be studied in later modules.

---

## Step 6 — Data is processed in partitions

Large distributed datasets are divided into partitions.

```text
sales.csv
   |
   +---- Partition 1
   +---- Partition 2
   +---- Partition 3
   +---- Partition 4
```

---

## Step 7 — Tasks are sent to executors

```text
Partition 1 → Task 1 → Executor 1
Partition 2 → Task 2 → Executor 2
Partition 3 → Task 3 → Executor 3
Partition 4 → Task 4 → Executor 1
```

The actual number of tasks and how they are scheduled depends on the application and execution plan.

---

## Step 8 — Executors perform the work

Executors process the assigned tasks using their allocated CPU and memory.

```text
Executor 1 → Task 1
Executor 2 → Task 2
Executor 3 → Task 3
```

---

## Step 9 — Status/results are reported

Executors communicate task status and relevant results back to the driver.

```text
Executor 1 ----\
Executor 2 -----+----> Driver
Executor 3 ----/
```

---

# 10. Driver vs Executor

This is one of the **most important Spark interview concepts**.

| Driver | Executor |
|---|---|
| Coordinates the Spark application | Performs computation |
| Runs the main application process | Runs tasks |
| Creates/schedules work | Executes assigned tasks |
| Tracks execution | Reports task status |
| Coordinates executors | Can cache/persist application data |
| Usually one driver per application | One or more executors per application |

### Easy memory trick

```text
DRIVER   = 🧠 BRAIN
EXECUTOR = 💪 MUSCLE
```

---

# 11. Worker Node vs Executor

Beginners often confuse these.

They are different.

### Worker Node

A **machine** that provides resources.

### Executor

A **process** running on a worker node for a particular Spark application.

```text
             Worker Node
          +----------------+
          | CPU            |
          | RAM            |
          | Storage        |
          |                |
          |   Executor     |
          +----------------+
```

A worker node can host multiple executors depending on the deployment and resource configuration.

---

# 12. Cluster Manager vs Worker Node

These are also different.

### Cluster Manager

Responsible for allocating resources to applications.

Examples:

```text
Spark Standalone
Hadoop YARN
Kubernetes
```

### Worker Node

A machine that provides the resources where application code/executors can run.

```text
              Cluster Manager
                     |
              Allocates Resources
                     |
        +------------+------------+
        |            |            |
     Worker 1     Worker 2     Worker 3
        |            |            |
    Executor     Executor     Executor
```

---

# 13. SparkSession

In modern PySpark applications, `SparkSession` is the main entry point for working with Spark's structured APIs and many Spark features.

Example:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MySparkApp")
    .getOrCreate()
)
```

Now `spark` can be used for operations such as:

```python
df = spark.read.csv("data.csv")
```

and:

```python
spark.sql("SELECT * FROM employees")
```

### Think of SparkSession as:

> 🚪 **The main entry point into Spark from your application.**

---

# 14. Job, Stage and Task — Preview

Spark's execution model uses several levels of work.

A simplified hierarchy is:

```text
Action
  |
  v
 Job
  |
  v
Stages
  |
  v
Tasks
```

### Job

A **job** is a parallel computation triggered by a Spark action.

Examples of actions include:

```python
df.show()
df.count()
df.collect()
df.write...
```

---

### Stage

A job is divided into stages.

Stages are groups of tasks that can execute together before a dependency boundary such as a shuffle.

---

### Task

A task is an individual unit of work sent to an executor.

```text
Job
 |
 +--- Stage 1
 |      |
 |      +--- Task
 |      +--- Task
 |      +--- Task
 |
 +--- Stage 2
        |
        +--- Task
        +--- Task
```

> We will study **Jobs → Stages → Tasks → DAG → Lazy Evaluation** in detail in Module 06.

---

# 15. Local Mode vs Cluster Mode

Spark can run locally on a single machine or across a cluster.

## Local Mode

Useful for:

- Learning
- Development
- Testing
- Small experiments

Example:

```bash
spark-submit --master local[2] app.py
```

Here Spark runs locally using two threads.

---

## Cluster Mode

In cluster mode, Spark uses resources from a cluster.

```text
              Spark Application
                     |
                   Driver
                     |
              Cluster Manager
                     |
          -----------------------
          |         |           |
       Worker    Worker      Worker
          |         |           |
       Executor  Executor   Executor
```

Spark supports deployment through:

- Standalone
- YARN
- Kubernetes

---

# 16. PySpark Example

## Create a Spark Application

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ArchitectureDemo")
    .getOrCreate()
)

data = [
    ("A", 100),
    ("B", 200),
    ("C", 300),
    ("D", 400)
]

columns = ["name", "sales"]

df = spark.createDataFrame(data, columns)

df.show()
```

### Output

```text
+----+-----+
|name|sales|
+----+-----+
|   A|  100|
|   B|  200|
|   C|  300|
|   D|  400|
+----+-----+
```

The important point is not the DataFrame syntax yet.

The important architecture is:

```text
Your Python Code
       |
       v
Spark Application
       |
       v
Driver
       |
       v
Spark Execution
       |
       v
Executors
       |
       v
Tasks
```

---

# 17. Spark Web UI

Spark provides a Web UI for monitoring applications.

The application's UI typically runs on port **4040** when the UI is enabled and available.

You can inspect information such as:

- Jobs
- Stages
- Tasks
- Executors
- Storage
- SQL/DataFrame queries
- Resource usage

Example:

```text
http://localhost:4040
```

### Official Spark Web UI

![Spark Web UI — Jobs](https://spark.apache.org/docs/latest/img/JobPage.png)

**Source:** [Apache Spark Web UI Documentation](https://spark.apache.org/docs/latest/web-ui.html)

The Web UI is extremely useful when debugging or optimizing Spark applications.

---

# 18. Important Terminology

| Term | Meaning |
|---|---|
| **Application** | User program built using Spark |
| **Driver** | Main coordinating process |
| **Cluster Manager** | Allocates resources for applications |
| **Worker Node** | Machine capable of running application code |
| **Executor** | Process running on a worker node for an application |
| **Task** | Unit of work sent to an executor |
| **Job** | Parallel computation triggered by an action |
| **Stage** | Group of tasks within a job |
| **Partition** | Logical chunk of distributed data |
| **SparkSession** | Main entry point for modern Spark applications |
| **Cluster** | Group of machines working together |

---

# 19. Key Takeaways

### ⭐ 1. Driver = Coordinator

The driver coordinates the Spark application and schedules work.

### ⭐ 2. Executor = Computation

Executors run tasks and can store cached/persisted data for the application.

### ⭐ 3. Worker Node = Machine

A worker node provides the resources on which executors run.

### ⭐ 4. Cluster Manager = Resource Allocation

The cluster manager allocates resources to Spark applications.

### ⭐ 5. Task = Unit of Work

Tasks are the actual pieces of work executed by executors.

### ⭐ 6. Application = Driver + Executors

A useful simplified mental model is:

```text
Spark Application
     |
     +---- Driver
     |
     +---- Executors
```

### ⭐ 7. Overall execution hierarchy

```text
Application
     ↓
Driver
     ↓
Cluster Manager
     ↓
Worker Nodes
     ↓
Executors
     ↓
Tasks
```

---

# 20. Module 02 Checklist

- [ ] Understand what a Spark Application is
- [ ] Understand the Driver
- [ ] Understand the Cluster Manager
- [ ] Understand Worker Nodes
- [ ] Understand Executors
- [ ] Understand Tasks
- [ ] Understand Driver vs Executor
- [ ] Understand Worker Node vs Executor
- [ ] Understand Cluster Manager vs Worker Node
- [ ] Understand SparkSession
- [ ] Understand the basic execution flow
- [ ] Know what a Job is
- [ ] Know what a Stage is
- [ ] Know what a Task is
- [ ] Understand Local Mode vs Cluster Mode
- [ ] Know where Spark Web UI fits into development

---

# 21. References

1. [Apache Spark — Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
2. [Apache Spark — Documentation](https://spark.apache.org/docs/latest/)
3. [Apache Spark — Web UI](https://spark.apache.org/docs/latest/web-ui.html)
4. [Apache Spark — Configuration](https://spark.apache.org/docs/latest/configuration.html)
5. [Apache Spark — Standalone Mode](https://spark.apache.org/docs/latest/spark-standalone.html)

---
![Description of image](image2.png)
