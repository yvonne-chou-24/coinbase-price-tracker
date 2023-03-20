# coinbase-price-tracker
Streaming pipeline which tracks and transforms cryptocurrency prices using Coinbase's public API. 

* Goal: Track live price changes to all cryptocurrencies available via Coinbase. Perform transformations on price changes and determine next steps depending on those price changes.
* Implementation: Create a stream which pulls all crypto currencies from Coinbase's API and writes to an object store and database using distributed systems. 
* Tech Stack: 
  * Kubernetes
  * Docker
  * Python
  * Kafka (Confluent)
  * Spark (AWS EMR)
  * Delta Lake
  * S3
  * DynamoDB
  
  
# Steps: 
  1. Write python script which uses asyncio to perform all http requests from Coinbase API. 
  2. Run script on a Kubernetes cluster using a Docker image. 
  3. Script writes to a Kafka topic, which is running via Confluent's managed services. 
  4. Use Spark in AWS EMR to consume from the Kafka topic and transform it accordingly. 
  5. Stream data to an object store (S3) and database (DynamoDB) for further transformations. 
