# Catalyst as a Way to Transport Logic from Apache Spark to External Systems



## Rationale

One of the challenges that developers face today in the world of big data machine learning applications is passing the computation logic or computation description around so that it can be used on independent platforms.
We will be exploring two main approaches:

1. [Catalyst Generated Java Code](#catalyst-generated-java-code)
2. [Catalyst Generated PFA Model](#catalyst-generated-pfa-model)



## Introduction

The main approach used so far was using the [Predictive Model Markup Language (PMML)](http://dmg.org/pmml/v4-3/GeneralStructure.html) standard from the [Data Mining Group (DMG)](http://dmg.org/) to communicate machine learning models.
PMML statically describes machine learning model and not the computation logic. 
The PMML standard showed quickly it's limitations and [Portable Format for Analytics (PFA)](http://dmg.org/pfa/) emerged as better standard standard to express computation rather than the model itself.

There are three main problems with using the PMML/PFA standards:

1. The translation of the existing code into the standard format.
2. The limitation in passing on user defined functions or already existing library functions from various libraries.
3. The model size limitation; which only becomes obvious when the model itself becomes "big data".
 
 
### 1. Code Translation

In Apache Spark many machine learning models already have a PMML implementation, but not all of them. 
This of course might change over time, but there are no guarantees.

On the other hand, the Catalyst logical plans conceptually map quite nicely to PFA, with the exception of library functions (`ScalaUDF`s), which can not be passed through PFA.

However, though there is some support for PMML export in Spark ML, there is no PFA support yet.


### 2. Library Functions

Even though PFA has support for expressing various transformations on data, which should make it fairly easy to express complex transformations, the library functions that are only be available in binary/byte code form can not be used. 

> Theoretically the byte code can be reversed engineered in some cases, but that is not a very ellegant solution.

> User defined functions for which the code is available can be theoretically parsed into Abstract Syntax Trees (ASTs) and translated into PFA format, however, extra effort is required. Along with this, a strict set of rules for writing functions needs to be enforced. The term *function* here is used rather in a mathematical sense rather than the general computer programming sense, where the function notion seems to be a little muddy.


### 3. Model Size Limitation

No matter the approach taken, large models will always present a challenge when communicating with external systems.
I have not seen yet a standard approach to communicating very large (big data) prediction models.



## Catalyst Generated Java Code

Apache Spark is using successfully on the fly code generation, compilation and execution, which brings us to the idea of intercepting the generated code and pass that along to describe the entire transformation process. 

The Apache Spark Catalyst engine unifies the description of transformations done on Spark Datasets into logical plans which are essentially ASTs. For all the intends and purposes, Catalyst is not a simple code generation tool, but a powerful SQL optimizer. However, the generated Java code is optimised for performance, being very flat, using no virtual function calls, a "strait shooter". This kind of code matches perfectly the world of high performance distributed computing over big data.

In order to produce even better code for the Spark environment, the logical plan is converted into a physical plan and the final Java code generated will be optimal for running by the Spark executors. 

For a initial attempt to produce "universal" code, rule-based optimized, the logical plan is a good starting point.

The code generated from the logical plan translates essentially to a function that takes an `InternalRow` as input and returns the result also as an `InternalRow`. The `InternalRow` can be perceived as an array of data.

The generated code is assembled into a `Projection` implementation. The `org.apache.spark.sql.catalyst.Projection` looks as following:

```scala
abstract class Projection extends (InternalRow => InternalRow)
```

In order to build the `Projection`, various parameters are required that are passed on to the constructor. These parameters mainly consist of the user defined functions used in the transformations and the type converter functions. However, both the code and the constructor parameters are build by the `CodeGenerator`, so they require minimal effort on the development side.

Having the generated class and the generated class instance, one only needs to pack them neatly together with the library dependencies and send them to the external system to be executed.

> For the moment I didn't look deep enough into Janino on how to compile on the fly and store the resulted **class** (not to be confused with the class instance) into a byte array form.

> The tricky part here is keeping a balance with the library functions used, as all the dependencies will have to be passed along. In the `KMeansGenExample` class, the serialization includes the `UnaryTransformer` from the `spark-mllib` library. All of the sudden the amount of dependencies that need to be passed on increases.

### Pros and Cons

**Pros**

- Java code generation out of the box
- The code and dependencies can be used in any JVM based system
- Easy to embed library functions in the process and deliver them along
- Easy to add external dependencies

**Cons**

- Limited by default to the JVM world
- Embedding 3rd party library functions can introduce security issues 
- The dependencies can easy to grow both in number and disk size
- Currently the Catalyst library depends on `spark-core`, making it hard to use by external systems
- Limited to simple transformations (no aggregation nor sub-query code generated from the logical plan directly)
- The generated code has dependencies to the Scala language


### Challenges

- Catalyst has dependencies to `spark-core`
  - A rough effort estimate to remove the dependency: 1-2 weeks
- Complex logical plans are not always serializable
  - A work around is shown in the `KMeansGenExample` using `CodeGenParameters`
- The generated code itself is not serializable
  - This can be solved by creating a custom code generator
  - Still looking into passing the generated class along 
- Assemble the code into jars or an uber-jar to be easy to used by external systems
- Figure out sub query code generation
- Figure out aggregation query code generation


### TODOs

- Verify code generation for joins and aggregations
- Look into generated complete implementations of physical plans



## Catalyst Generated PFA Model

PFA is self described as "a mini-language for mathematical calculations that is usually generated programmatically, rather than by hand".

PFA has a "*LISPy*" like syntax, which has the advantage that makes it really easy to represent expressions as trees, using the prefix notation. This is a perfect match for the Catalyst `TreeNode` based expressions hierarchy.



### Pros and Cons

**Pros**

- Very suitable for code generation, in both directions, from a programming language to PFa and from PFA to a programming language.
- High security due to it's limitations to a fairly simple mathematical language, which allows no external library calls, so the PFA code can not introduce security issues.
- Enforces high discipline in writing "analytics code".
- Possible just a light dependency to a PFA parser library, which is already available (see  [Hadrian: implementations of the PFA specification](https://github.com/opendatagroup/hadrian))

**Cons**

- Not human friendly, so it can not be used directly by data scientists.
- Library functions need to be ported in an expression for that can be directly translated into PFA.
- Spark has no PFA support so using PFA requires substantial additional development.



## Conclusions

Spark Catalyst proves to be a very powerful tool that can grow beyond the current usage in Spark.

Weather is Java source code generation or other language, like PFA, the abstractions provided by the Spark Catalyst are very helpful.



## References

- [Spark Release 2.0.0](https://spark.apache.org/releases/spark-release-2-0-0.html)

- [Portable Format for Analytics (PFA)](http://dmg.org/pfa/)

- [PFA version 0.8.1 Specification](http://github.com/datamininggroup/pfa/releases/download/0.8.1/pfa-specification.pdf)

- [Hadrian: implementations of the PFA specification](https://github.com/opendatagroup/hadrian)

- [Predictive Model Markup Language (PMML)](http://dmg.org/pmml/v4-3/GeneralStructure.html)

- [Spark Catalyst Description and Usage](./catalyst-description.md)
