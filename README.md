# spark-powerbi
A library for pushing data from Spark, Spark SQL, or Spark Streaming to [Power BI](https://powerbi.com/).

## Requirements
This library is built for Spark 1.2+

## API
NOTE: The Power BI API is still in preview so this library will change as new features are added. Additional details are available in the [developer center](https://msdn.microsoft.com/en-us/library/dn877544.aspx). Authentication is handled via OAuth2 with your Azure AD credentials specified via Spark properties. More details on registering an app and authenticating are available in the Power BI dev center. When pushing rows to Power BI the library will create the target dataset with table if necessary. The current Power BI service is limited to 10,000 rows per call so the library handles batching internally. The service also limits to no more than 5 concurrent calls at a time when adding rows. This needs to be handled by the driver by limiting the partitions such as using coalesce if necessary.

## Configuration
A few of the key properties are related to OAuth2. These depend on your application's registration in Azure AD.
```
spark.powerbi.username
spark.powerbi.password
spark.powerbi.clientid
```

## Spark Core
```
import com.granturing.spark.powerbi._

case class Person(name: String, age: Int)

val input = sc.textFile("examples/src/main/resources/people.txt")
val people = input.map(_.split(",")).map(l => Person(l(0), l(1).trim.toInt))

people.saveToPowerBI("Test", "People")
```

## SparkSQL
```
import com.granturing.spark.powerbi._
import org.apache.spark.sql._

val sqlCtx = new SQLContext(sc)
val people = sqlCtx.jsonFile("examples/src/main/resources/people.json")

people.saveToPowerBI("Test", "People")
```

## Spark Streaming
```
val sc = new SparkContext(new SparkConf())
val ssc = new StreamingContext(sc, Seconds(5))

val filters = args

val input = TwitterUtils.createStream(ssc, None, filters)

val tweets = input.map(t => Tweet(t.getId, t.getCreatedAt, t.getText, t.getUser.getScreenName))
val hashTags = input.flatMap(t => t.getHashtagEntities.map(h => HashTag(t.getId, h.getText, t.getUser.getScreenName)))

tweets.saveToPowerBI(dataset, "Tweets")
hashTags.saveToPowerBI(dataset, "HashTags")

ssc.start()
ssc.awaitTermination()
```

## Building From Source
The library uses SBT and can easily be built by running ```sbt package``` to generate a JAR file or ```sbt assembly``` to generate an assembly JAR with necessary dependencies.
