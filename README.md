# Insurance-Prediction-Pyspark
## _Insurance Prediction by using Pyspark and Kafka Streaming_

[![N|Solid](https://www.snaplogic.com/wp-content/uploads/2016/05/kafka-logo-600x390.jpg)](https://kafka.apache.org/)

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
