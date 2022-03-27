# Insurance-Prediction-Pyspark
## _Insurance Prediction by using Pyspark and Kafka Streaming_

[![N|Solid](data:image/png;base64,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)](https://kafka.apache.org/)

[![N|Solid](https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png)](https://spark.apache.org/)

This project is based on Pyspark for dealing with the problem of big data and stream data analysis. Here I integerate Kafka with Pyspark.


## Features

. The features that I deals with in Insurance Prediction Datasets are:

- age
- sex
- bmi
- children
- smoker
- region
- expenses



## Installation

Python version==3.6.9

Install the dependencies

```sh
pip install -r requirements.txt
```

Setting the environment Variable...

```sh
export MONGO_USER="(write the mongodb attlas user name)"
MONGO_PASSWORD="(write the mongodb atlas password)"
```

## Plugins

Set Spark in Ubuntu 2-0.04 LTS.
```sh
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/home/user/anaconda3/bin/python3
```

## Create conda environment 

1. open conda terminal execute below command

```buildoutcfg
conda create -n <env_name> python=3.6.9 -y
```

2. select <env_name>
```buildoutcfg
conda activate <env_name>
```
3. Install all necessary python library specified in requirements.txt file using below command.
```buildoutcfg
pip install -r requirements.txt
```

## Train random forest model on insurance dataset
```buildoutcfg
python training/data_loader_stage_00.py
```
```buildoutcfg
python training/data_validation_01.py
```
```buildoutcfg
python training/data_transformation__stage_02.py
```
```buildoutcfg
python training/stage_03_data_exporter.py
```
```buildoutcfg
python training/data_training_stage_04.py
```

## Prediction using random forest of insurance dataset
```buildoutcfg
python prediction/data_loader_stage_00.py
```
```buildoutcfg
python prediction/data_validation_01.py
```
```buildoutcfg
python prediction/data_transformation__stage_02.py
```
```buildoutcfg
python prediction/stage_03_data_exporter.py
```
```buildoutcfg
spark-submit execute_batch_prediction.py
```



# start zookeeper and kafka server



## start kafka producer using below command
```buildoutcfg
spark-submit csv_to_kafka.py
```

## start pyspark consumer using below command
```buildoutcfg
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  kafka_consumer.py
```
## License

MIT License

Copyright (c) 2022 Waqas Bilal

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)

   [dill]: <https://github.com/joemccann/dillinger>
   [git-repo-url]: <https://github.com/joemccann/dillinger.git>
   [john gruber]: <http://daringfireball.net>
   [df1]: <http://daringfireball.net/projects/markdown/>
   [markdown-it]: <https://github.com/markdown-it/markdown-it>
   [Ace Editor]: <http://ace.ajax.org>
   [node.js]: <http://nodejs.org>
   [Twitter Bootstrap]: <http://twitter.github.com/bootstrap/>
   [jQuery]: <http://jquery.com>
   [@tjholowaychuk]: <http://twitter.com/tjholowaychuk>
   [express]: <http://expressjs.com>
   [AngularJS]: <http://angularjs.org>
   [Gulp]: <http://gulpjs.com>

   [PlDb]: <https://github.com/joemccann/dillinger/tree/master/plugins/dropbox/README.md>
   [PlGh]: <https://github.com/joemccann/dillinger/tree/master/plugins/github/README.md>
   [PlGd]: <https://github.com/joemccann/dillinger/tree/master/plugins/googledrive/README.md>
   [PlOd]: <https://github.com/joemccann/dillinger/tree/master/plugins/onedrive/README.md>
   [PlMe]: <https://github.com/joemccann/dillinger/tree/master/plugins/medium/README.md>
   [PlGa]: <https://github.com/RahulHP/dillinger/blob/master/plugins/googleanalytics/README.md>
