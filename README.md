# target-partition-producer

## Required
A `*.properties` file with the bare minimum defined:

```properties
# include security related configs if needed (security.protocol, sasl.mechanism, ...)
bootstrap.servers=localhost:9092

# how many records to send
app.target.count=1

# topic to produce to
app.target.topic=test.topic.tpp

# partition to produce to  
app.target.partition=0
```
> NOTE: `app.*` property values shown above are defaults - ie. if `app.target.count` is not defined, value will default to `1`   




### Run

- `java -jar /path/to/tpp.jar /path/to/application.properties` 
- or (from root project dir)
- `java -jar target/tpp.jar src/main/resources/application.properties`




