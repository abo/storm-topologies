# Storm Topologies 

## - Chinese Word Count

using [jieba](https://github.com/fxsjy/jieba) for segment.

### Run `WordCountTopology` in local mode, use this command:

```
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.trs.smas.storm.topology.WordCount
```


### Package a jar suitable for submitting to a cluster with this command:
```
mvn package
```

This will package your code and all the non-Storm dependencies into a single "uberjar" at the path `target/storm-topologies-{version}-jar-with-dependencies.jar`.
