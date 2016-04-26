# WikiLDA

==Requirements==  
Spark 1.5+  
Maven 3.0+  
Hadoop 2.6  


1. Create a Spark cluster on VCL by following the instructions at [this](https://github.com/amritbhanu/Spark_VCL) repository.
2. Download the English Wikipedia XML BZip2 file. It can be downloaded in one of the two ways:  
  a) as torrent from [dump torrents] (https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki)  
  b) direct download as multiple BZIP2 streams from the [dump](link https://dumps.wikimedia.org/enwiki/)
3. Uncompress the bzip2 file using the below command. The uncompressed file is over 50GB. So make sure there is enough disk space  
```bzip2 -dk <filename>.bz2```
4. Tranfer the uncompressed XML file to HDFS using the below command. This may take a lot of time (well over 8 hours).
5. 2. Git clone this repository.
6. Run mvn package to build an assembly jar. It will download parent dependencies as well the dependencies for modules lda & xml.
7. After running the above step, an assembly jar LDA-1.0.2-jar-with-dependencies.jar should be present in lda/target folder. Transfer this jar to Spark master.
8. ssh to Spark master and submit the Spark job.

