## goals

Build up an OLAP system based on Spark.

Spark is just the distributed calculating framework, so you need to make "orchestration" by yourself. So I build up the offline server as some tool, with it to organize spark jobs together to complete OLAP calculation.

However, how to calculate is more important, offline server not only aims to organize jobs, but also to define or improve OLAP models. 


todo:: add aggregation to api, which is the key to open offline-server.

## OLAP

I believe in a common bigdata system, some fields requires accuracy, but it is not necessary to keep all data totally accurate. We allow a small error rate in trending analysis. So, before starting analysis, we must define which data resource requires high precision, and which not. BigData System is not some stupid-violence calculation, it is an Algorithm System, which aims to solve realistic demands and problems.