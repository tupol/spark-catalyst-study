# Spark Catalyst Study



This project started as a research to find alternative ways of passing computation logic from Spark to external, 
independent systems (like Apache Flink).

The main idea is to be able to pass complicated input transformation and if possible the prediction transformations
done by the machine learning models in Spark, as code, binary or serialised plans (ASTs).


[Catalyst as a Way to Transport Logic from Apache Spark to External Systems](docs/catalyst-transport.md)

[Spark Catalyst Description and Usage](docs/catalyst-description.md)
