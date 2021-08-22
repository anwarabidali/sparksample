```pyspark
df = spark.read.csv("s3://airportsparkcsvdata/Airports2.csv", header=True, inferSchema=True)
df.registerTempTable('df')
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
df.printSchema()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    root
     |-- Origin_airport: string (nullable = true)
     |-- Destination_airport: string (nullable = true)
     |-- Origin_city: string (nullable = true)
     |-- Destination_city: string (nullable = true)
     |-- Passengers: integer (nullable = true)
     |-- Seats: integer (nullable = true)
     |-- Flights: integer (nullable = true)
     |-- Distance: integer (nullable = true)
     |-- Fly_date: string (nullable = true)
     |-- Origin_population: integer (nullable = true)
     |-- Destination_population: integer (nullable = true)
     |-- Org_airport_lat: string (nullable = true)
     |-- Org_airport_long: string (nullable = true)
     |-- Dest_airport_lat: string (nullable = true)
     |-- Dest_airport_long: string (nullable = true)


```pyspark
df.describe().show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------+--------------+-------------------+--------------+----------------+------------------+-----------------+-----------------+-----------------+----------+-----------------+----------------------+------------------+------------------+-----------------+------------------+
    |summary|Origin_airport|Destination_airport|   Origin_city|Destination_city|        Passengers|            Seats|          Flights|         Distance|  Fly_date|Origin_population|Destination_population|   Org_airport_lat|  Org_airport_long| Dest_airport_lat| Dest_airport_long|
    +-------+--------------+-------------------+--------------+----------------+------------------+-----------------+-----------------+-----------------+----------+-----------------+----------------------+------------------+------------------+-----------------+------------------+
    |  count|       3606803|            3606803|       3606803|         3606803|           3606803|          3606803|          3606803|          3606803|   3606803|          3606803|               3606803|           3606803|           3606803|          3606803|           3606803|
    |   mean|          null|               null|          null|            null|2688.9104331453646|4048.297368888736|37.22889855642241|697.3190326724249|      null| 5871502.49894491|      5897982.44118434|37.750289955901664|-91.86177935715813|37.74090832973531|-91.83432738662697|
    | stddev|          null|               null|          null|            null|  4347.61704769634|6200.871210153884|49.61969779949641| 604.416510848349|      null| 7858061.60182103|      7906127.40640526| 5.765453283976699| 16.53773370268353|5.736555536048648|16.472284529884853|
    |    min|           1B1|                1B1|  Aberdeen, SD|    Aberdeen, SD|                 0|                0|                0|                0|1990-01-01|            13005|                 12887|   19.721399307251|      -100.2860031|  19.721399307251|      -100.2860031|
    |    max|           ZZV|                ZZV|Zanesville, OH|  Zanesville, OH|             89597|           147062|             1128|             5095|2009-12-01|         38139592|              38139592|                NA|                NA|               NA|                NA|
    +-------+--------------+-------------------+--------------+----------------+------------------+-----------------+-----------------+-----------------+----------+-----------------+----------------------+------------------+------------------+-----------------+------------------+


```pyspark
df.count()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    3606803


```pyspark
df.select("Origin_airport","Destination_airport","Passengers","Seats").show(15)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------+-------------------+----------+-----+
    |Origin_airport|Destination_airport|Passengers|Seats|
    +--------------+-------------------+----------+-----+
    |           MHK|                AMW|        21|   30|
    |           EUG|                RDM|        41|  396|
    |           EUG|                RDM|        88|  342|
    |           EUG|                RDM|        11|   72|
    |           MFR|                RDM|         0|   18|
    |           MFR|                RDM|        11|   18|
    |           MFR|                RDM|         2|   72|
    |           MFR|                RDM|         7|   18|
    |           MFR|                RDM|         7|   36|
    |           SEA|                RDM|         8|   18|
    |           SEA|                RDM|       453| 3128|
    |           SEA|                RDM|       784| 2720|
    |           SEA|                RDM|       749| 2992|
    |           SEA|                RDM|        11|   18|
    |           PDX|                RDM|       349|  851|
    +--------------+-------------------+----------+-----+
    only showing top 15 rows


```pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import desc

airportAgg_DF = df.groupBy("Origin_airport").agg(F.sum("Passengers"))
airportAgg_DF.show(10)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------+---------------+
    |Origin_airport|sum(Passengers)|
    +--------------+---------------+
    |           BGM|        1876537|
    |           VWD|              0|
    |           GEG|       23872254|
    |           OPF|           7534|
    |           MYR|       10386029|
    |           CNW|              0|
    |           EAR|           2548|
    |           MQT|         720004|
    |           YKN|              0|
    |           GWO|             45|
    +--------------+---------------+
    only showing top 10 rows


```pyspark

originAirports = sqlContext.sql("""select Origin_Airport, sum(Flights) as Flights 
                                    from df group by Origin_Airport order by sum(Flights) DESC limit 10""")
originAirports.show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------+-------+
    |Origin_Airport|Flights|
    +--------------+-------+
    |           ORD|6908482|
    |           ATL|6558015|
    |           DFW|5994638|
    |           LAX|4099901|
    |           DTW|3452613|
    |           PHX|3213108|
    |           MSP|3204923|
    |           IAH|3195062|
    |           STL|3181102|
    |           CLT|2840773|
    +--------------+-------+


```pyspark
destinationAirports = sqlContext.sql("""select Destination_airport, sum(Passengers) as Passengers 
                                    from df group by Destination_airport order by sum(Passengers) DESC limit 10""")
destinationAirports.show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------------------+----------+
    |Destination_airport|Passengers|
    +-------------------+----------+
    |                ATL| 577954147|
    |                ORD| 528648148|
    |                DFW| 458322527|
    |                LAX| 389476602|
    |                PHX| 295580444|
    |                LAS| 269145891|
    |                DTW| 251467874|
    |                MSP| 245774036|
    |                SFO| 242283245|
    |                IAH| 229105403|
    +-------------------+----------+


```pyspark
MostFlightsByAirports = sqlContext.sql("""with destination as (select Destination_airport as Airport, sum(Flights) as Out_Flights 
                                    from df group by Destination_airport),
                                    origin as (select Origin_airport as Airport, sum(Flights) as In_Flights 
                                    from df group by Origin_airport)
                                    select origin.Airport, (destination.Out_Flights+origin.In_Flights) as Total_Flights
                                    from origin, destination 
                                    where origin.Airport = destination.Airport
                                    order by (origin.In_Flights + destination.Out_Flights) DESC
                                    limit 15""")
MostFlightsByAirports.show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------+-------------+
    |Airport|Total_Flights|
    +-------+-------------+
    |    ORD|     13804767|
    |    ATL|     13102774|
    |    DFW|     11982524|
    |    LAX|      8196603|
    |    DTW|      6900655|
    |    PHX|      6421985|
    |    MSP|      6405105|
    |    IAH|      6391830|
    |    STL|      6358741|
    |    CLT|      5677880|
    |    EWR|      5620104|
    |    LGA|      5368743|
    |    PHL|      5344278|
    |    SEA|      5158456|
    |    LAS|      5040914|
    +-------+-------------+


```pyspark

MostPassengersByAirports = sqlContext.sql("""with destination as (select Destination_airport as Airport, sum(Passengers*Flights) as Out_Passengers 
                                    from df group by Destination_airport),
                                    origin as (select Origin_airport as Airport, sum(Passengers) as In_Passengers
                                    from df group by Origin_airport)
                                    select origin.Airport, (destination.Out_Passengers+origin.In_Passengers) as Total_Passengers
                                    from origin, destination 
                                    where origin.Airport = destination.Airport
                                    order by (origin.In_Passengers + destination.Out_Passengers) DESC
                                    limit 15""")
MostPassengersByAirports.show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------+----------------+
    |Airport|Total_Passengers|
    +-------+----------------+
    |    DFW|     64193553706|
    |    ATL|     62429740643|
    |    ORD|     56896329462|
    |    LAX|     53533015275|
    |    LGA|     32947558489|
    |    PHX|     32559529060|
    |    LAS|     29100039374|
    |    SFO|     27096863160|
    |    BOS|     24112128142|
    |    SEA|     23575268222|
    |    HNL|     23235437905|
    |    DCA|     20108810076|
    |    MCO|     19771422559|
    |    IAH|     19289444015|
    |    STL|     19122558315|
    +-------+----------------+


```pyspark
distanceQuery = sqlContext.sql("""with table1 as 
                                    (select least(Origin_airport, Destination_airport) as Airport1, 
                                    greatest(Destination_airport, Origin_airport) as Airport2, 
                                    sum(Flights) as Flights,
                                    sum(Passengers) as Passengers,
                                    sum(Seats) as Seats
                                    from df
                                    group by least(Origin_airport, Destination_airport), greatest(Destination_airport, Origin_airport)
                                    order by 1,2)
                                    select t.*, (Passengers*100/Seats) as Occupancy_Rate
                                    from table1 t
                                    order by Flights DESC, Seats DESC, Passengers DESC, Occupancy_Rate DESC
                                    limit 15;""")
distanceQuery = distanceQuery.filter((col("Occupancy_Rate").isNotNull()) & (col("Occupancy_Rate")<=100.0))
distanceQuery.show(15)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------+--------+-------+----------+--------+------------------+
    |Airport1|Airport2|Flights|Passengers|   Seats|    Occupancy_Rate|
    +--------+--------+-------+----------+--------+------------------+
    |     HNL|     OGG| 784873|  62109354|96640901| 64.26818599300931|
    |     LAX|     SFO| 636449|  51119989|79405656|  64.3782717442697|
    |     LAS|     LAX| 588151|  52511530|80532768| 65.20517213564546|
    |     PDX|     SEA| 565707|  18475771|34650955| 53.31965886654495|
    |     LAX|     PHX| 515093|  42695385|65619395| 65.06519147273455|
    |     BOS|     LGA| 470737|  31242486|64897330| 48.14140427657039|
    |     MSP|     ORD| 467514|  31301666|55325318|56.577471457100344|
    |     LAS|     PHX| 460104|  42979048|64844100| 66.28058373853597|
    |     DCA|     LGA| 439107|  29471657|60663368|   48.582295991215|
    |     LAX|     SAN| 431076|  11686171|22820096|51.209999291852235|
    |     LGA|     ORD| 424272|  39981416|59616532| 67.06431028225526|
    |     DAL|     HOU| 408273|  35573141|53054549| 67.05012420329876|
    |     ATL|     DFW| 399696|  42941213|59978776| 71.59401352238332|
    |     LAX|     OAK| 381677|  30429561|47045709| 64.68084262477583|
    |     EWR|     ORD| 372054|  31122785|50434853| 61.70888413216947|
    +--------+--------+-------+----------+--------+------------------+


```pyspark
distanceQuery = sqlContext.sql("""with table1 as 
                                    (select least(Origin_airport, Destination_airport) as Airport1, 
                                    greatest(Destination_airport, Origin_airport) as Airport2, 
                                    mean(Distance) as Distance,
                                    sum(Flights) as Flights
                                    from df
                                    group by least(Origin_airport, Destination_airport), greatest(Destination_airport, Origin_airport)
                                    order by 1,2)
                                    select t.*
                                    from table1 t
                                    where Flights>0
                                    order by Distance DESC
                                    limit 15;""")
# distanceQuery = distanceQuery.filter((col("Occupancy_Rate").isNotNull()) & (col("Occupancy_Rate")<=100.0))
distanceQuery.show(15)

```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------+--------+--------+-------+
    |Airport1|Airport2|Distance|Flights|
    +--------+--------+--------+-------+
    |     BDL|     HNL|  5018.0|      3|
    |     HNL|     JFK|  4983.0|    430|
    |     HIK|     JFK|  4983.0|      3|
    |     HNL|     LGA|  4976.0|      1|
    |     EWR|     HNL|  4962.0|   8320|
    |     JFK|     OGG|  4924.0|      1|
    |     HNL|     PHL|  4919.0|      1|
    |     HNL|     MIA|  4862.0|      9|
    |     HIK|     MIA|  4862.0|      1|
    |     HNL|     ISO|  4860.0|      1|
    |     OGG|     PHL|  4859.0|      1|
    |     BWI|     HNL|  4855.0|      7|
    |     HNL|     IAD|  4817.0|      3|
    |     BWI|     OGG|  4793.0|      1|
    |     HIK|     POB|  4785.0|      1|
    +--------+--------+--------+-------+


```pyspark
distanceQuery = sqlContext.sql("""with table1 as 
                                    (select least(Origin_airport, Destination_airport) as Airport1, 
                                    greatest(Destination_airport, Origin_airport) as Airport2, 
                                    mean(Distance) as Distance,
                                    sum(Flights) as Flights
                                    from df
                                    group by least(Origin_airport, Destination_airport), greatest(Destination_airport, Origin_airport)
                                    order by 1,2)
                                    select t.*
                                    from table1 t
                                    where Flights>0
                                    order by Flights DESC
                                    limit 15;""")
# distanceQuery = distanceQuery.filter((col("Occupancy_Rate").isNotNull()) & (col("Occupancy_Rate")<=100.0))
distanceQuery.show(15)

```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------+--------+------------------+-------+
    |Airport1|Airport2|          Distance|Flights|
    +--------+--------+------------------+-------+
    |     HNL|     OGG|             100.0| 784873|
    |     LAX|     SFO|             337.0| 636449|
    |     LAS|     LAX|             236.0| 588151|
    |     PDX|     SEA|             129.0| 565707|
    |     LAX|     PHX|             370.0| 515093|
    |     BOS|     LGA|             185.0| 470737|
    |     MSP|     ORD|             334.0| 467514|
    |     LAS|     PHX|255.96021840873635| 460104|
    |     DCA|     LGA|             214.0| 439107|
    |     LAX|     SAN|             109.0| 431076|
    |     LGA|     ORD|             733.0| 424272|
    |     DAL|     HOU|             239.0| 408273|
    |     ATL|     DFW| 731.9746309301993| 399696|
    |     LAX|     OAK|             337.0| 381677|
    |     EWR|     ORD|             719.0| 372054|
    +--------+--------+------------------+-------+


```pyspark

```
