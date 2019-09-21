# Apache-HFT
Open Source High Frequency Trading Bot


# Introduction
Apache-HFT is an open source algorithmic trading bot designed for (relatively) high speed algorithmic trading. There are multiple ways to interface with the trading engines including a web UI and a custom API. The trading engine is built to be able to handle the needs of trading algorithms needing precision down to 10 microseconds, any trading system requiring speeds faster than this should not rely on this
engine and probably should look into using FPGAs for processing quotes from a colocated datacenter. 


# Backends

| Backend   | Providers | 
|----------:|----------:|
| Database  | MongoDB   |
| AMQP      | RabbitMQ / ZeroMQ / Celery |





# Handling Quotes & Building Orderbooks
The world of algorithmic trading has seen a huge increase in the amount of data produced per second over the last 10 years, and can put a huge strain on processing engines needing to run 24/7 for markets such as forex and crypto. To handle the high volume of data, there are a couple of different options you can choose from including database backends, csv files, and SQLite3. Using a cloud hosted database is by far the best option you could choose, and some options are even very simple to set up like MongoDB for example. The option you choose however will be dependent upon what your needs are in terms of speed, volume of data, storage, and the level of complexity you are willing to undertake for optimizing your quote processing. While a truly optimized setup might include using redis key value stores in memory across a multitude of machines before storing the data in a PostgreSQL db optimized for timeseries data such as TimescaleDB, this can be a very complicated development process as many different considerations must be made for eliminating duplicated data and replicating streams of inserts and updates across multiple machines. However; this luckily is not the only way a high speed data processing engine can be created. MongoDB is another option that can replace the more complex one above. MongoDB is a document based NoSQL database that uses JSON formatting that allows for very flexible formatting. If you are not familiar with MongoDB, you may find their documentation here [https://docs.mongodb.com/] very helpful 
