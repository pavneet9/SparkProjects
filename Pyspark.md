

```
from pyspark import SparkContext, SQLContext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

train = sqlContext.read.csv( path = 'gs://dataproc-staging-us-central1-704334322887-f8nn09qg/notebooks/jupyter/airports.csv', header = True,inferSchema = True)
```


```
train.show()
```

    +---------+--------------------+--------------+----------------+---+----+---------+----------+--------+--------+---+--------------------+
    |AirportID|                Name|          City|         Country|FAA|ICAO| Latitude| Longitude|Altitude|Timezone|DST|                  Tz|
    +---------+--------------------+--------------+----------------+---+----+---------+----------+--------+--------+---+--------------------+
    |        1|              Goroka|        Goroka|Papua New Guinea|GKA|AYGA|-6.081689|145.391881|    5282|    10.0|  U|Pacific/Port_Moresby|
    |        2|              Madang|        Madang|Papua New Guinea|MAG|AYMD|-5.207083|  145.7887|      20|    10.0|  U|Pacific/Port_Moresby|
    |        3|         Mount Hagen|   Mount Hagen|Papua New Guinea|HGU|AYMH|-5.826789|144.295861|    5388|    10.0|  U|Pacific/Port_Moresby|
    |        4|              Nadzab|        Nadzab|Papua New Guinea|LAE|AYNZ|-6.569828|146.726242|     239|    10.0|  U|Pacific/Port_Moresby|
    |        5|Port Moresby Jack...|  Port Moresby|Papua New Guinea|POM|AYPY|-9.443383| 147.22005|     146|    10.0|  U|Pacific/Port_Moresby|
    |        6|          Wewak Intl|         Wewak|Papua New Guinea|WWK|AYWK|-3.583828|143.669186|      19|    10.0|  U|Pacific/Port_Moresby|
    |        7|          Narsarsuaq|  Narssarssuaq|       Greenland|UAK|BGBW|61.160517|-45.425978|     112|    -3.0|  E|     America/Godthab|
    |        8|                Nuuk|      Godthaab|       Greenland|GOH|BGGH|64.190922|-51.678064|     283|    -3.0|  E|     America/Godthab|
    |        9|   Sondre Stromfjord|   Sondrestrom|       Greenland|SFJ|BGSF|67.016969|-50.689325|     165|    -3.0|  E|     America/Godthab|
    |       10|      Thule Air Base|         Thule|       Greenland|THU|BGTL|76.531203|-68.703161|     251|    -4.0|  E|       America/Thule|
    |       11|            Akureyri|      Akureyri|         Iceland|AEY|BIAR|65.659994|-18.072703|       6|     0.0|  N|  Atlantic/Reykjavik|
    |       12|         Egilsstadir|   Egilsstadir|         Iceland|EGS|BIEG|65.283333|-14.401389|      76|     0.0|  N|  Atlantic/Reykjavik|
    |       13|        Hornafjordur|          Hofn|         Iceland|HFN|BIHN|64.295556|-15.227222|      24|     0.0|  N|  Atlantic/Reykjavik|
    |       14|             Husavik|       Husavik|         Iceland|HZK|BIHU|65.952328|-17.425978|      48|     0.0|  N|  Atlantic/Reykjavik|
    |       15|          Isafjordur|    Isafjordur|         Iceland|IFJ|BIIS|66.058056|-23.135278|       8|     0.0|  N|  Atlantic/Reykjavik|
    |       16|Keflavik Internat...|      Keflavik|         Iceland|KEF|BIKF|   63.985|-22.605556|     171|     0.0|  N|  Atlantic/Reykjavik|
    |       17|      Patreksfjordur|Patreksfjordur|         Iceland|PFJ|BIPA|65.555833|   -23.965|      11|     0.0|  N|  Atlantic/Reykjavik|
    |       18|           Reykjavik|     Reykjavik|         Iceland|RKV|BIRK|    64.13|-21.940556|      48|     0.0|  N|  Atlantic/Reykjavik|
    |       19|        Siglufjordur|  Siglufjordur|         Iceland|SIJ|BISI|66.133333|-18.916667|      10|     0.0|  N|  Atlantic/Reykjavik|
    |       20|      Vestmannaeyjar|Vestmannaeyjar|         Iceland|VEY|BIVM|63.424303|-20.278875|     326|     0.0|  N|  Atlantic/Reykjavik|
    +---------+--------------------+--------------+----------------+---+----+---------+----------+--------+--------+---+--------------------+
    only showing top 20 rows
    


Let us now convert this data frame into a temp view so that we can query against it


```
train.createTempView("airport")
```

Let's find out how many airports are there in South east part in our dataset


```
output = sqlContext.sql("select AirportID, Name, Latitude, Longitude from airport where Latitude<0 and Longitude>0")
```

Count the number of Airports in each city


```
sqlContext.sql("select Country, count(distinct(City)) from airport group by Country").show()
```


```
output.write.csv('airportsByCountry.csv')
```
