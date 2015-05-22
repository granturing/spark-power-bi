# spark-power-bi
A library for pushing data from Spark, SparkSQL, or Spark Streaming to [Power BI](https://powerbi.com/).

## Requirements
This library is built for Spark 1.2 and 1.3. The versions of the library match to the Spark version. So v1.2.0_0.0.2 is for Spark 1.2 & v1.3.0_0.0.3 is for Spark 1.3.

## Power BI API
NOTE: The Power BI API is still in preview so this library will change as new features are added. Additional details are available in the [developer center](https://msdn.microsoft.com/en-us/library/dn877544.aspx). Authentication is handled via OAuth2 with your Azure AD credentials specified via Spark properties. More details on registering an app and authenticating are available in the Power BI dev center. When pushing rows to Power BI the library will create the target dataset with table if necessary. The current Power BI service is limited to 10,000 rows per call so the library handles batching internally. The service also limits to no more than 5 concurrent calls at a time when adding rows. This is handled by the library using coalesce and can be tuned by with the `spark.powerbi.max_partitions` property.

## Scaladoc
Scaladoc is available [here](http://granturing.github.io/spark-power-bi/docs)

## Configuration
A few of the key properties are related to OAuth2. These depend on your application's registration in Azure AD.
```
spark.powerbi.username
spark.powerbi.password
spark.powerbi.clientid
```

Rather than using your personal AD credentials for publishing data, you may want to create a service account instead. Then you can logon to Power BI using that account and share the data sets and dashboards with other users in your organization. Unfortunately, there's currently no other way of authenticating to Power BI. Hopefully in the future there'll be an organization-level API token that can publish shared data sets, without having to use an actual AD account.

### Setting Up Azure Active Directory
You'll need to create an application within your Azure AD in order to have a client id to publish data sets.

1. Using the Azure management portal, open up your directory and add a new Application (under the Apps tab)
2. Select "Add an application my organization is developing"
![step0](http://granturing.github.io/spark-power-bi/images/AD_Setup_0.png)
3. Enter any name you want and select "Native Client Application"
![step1](http://granturing.github.io/spark-power-bi/images/AD_Setup_1.png)
4. Enter a redirect URI, this can be anything since it won't be used
![step2](http://granturing.github.io/spark-power-bi/images/AD_Setup_2.png)
5. Once the app has been added you need to grant permissions, click "Add application"
![step3](http://granturing.github.io/spark-power-bi/images/AD_Setup_3.png)
6. Select the "Power BI Service"
![step4](http://granturing.github.io/spark-power-bi/images/AD_Setup_4.png)
7. Add all 3 of the delegated permissions
![step5](http://granturing.github.io/spark-power-bi/images/AD_Setup_5.png)
8. Save your changes and use the newly assigned client id for the `spark.powerbi.clientid` property
![step6](http://granturing.github.io/spark-power-bi/images/AD_Setup_6.png)

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

## Referencing As A Dependency
With Spark 1.3 you can now easily reference dependencies using the `--packages` argument:
```bash
spark-shell --package com.granturing:spark-power-bi_2.10:1.3.0_0.0.3
```

## Building From Source
The library uses SBT and can easily be built by running ```sbt package```.
