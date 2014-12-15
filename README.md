
                                 DataTunnel
![Image text](https://github.com/ganqiang1983/DataTunnel/blob/master/logo.gif)

  What is it? <br />
  DataTunnel是一款基于异构存储的数据处理管道，可以十分方便用于两个同构或异构存储  <br /> 
之间的数据同步，数据统计等数据处理工作。同时，它具有以下特点：  <br /> 
1.集成多类型存储：Mysql/Oracle/Hive/HDFS/HBase/MongoDB/Redis <br /> 
2.简单配置：关系性数据库只需要配置Sql，Hive需要配置hql，就可以直接运行， <br /> 
                          而其他类型存储目前版本需要实现接口。 <br /> 
3.可扩展编程接口：复杂的应用可以自定义接口程序，只需实现Readable和Writeable接口即可。 <br /> 
4.多任务支持：支持多个任务在多线程环境下同时运行。 <br /> 

  -----------

  The Current Version : 1.0-beta

  -----------

  Package : <br />
  mvn clean package
  
  -----------

  Run : <br />
  cd target<br />
  tar xvf dbstat-version-bin.tar.gz<br />
  chmod -R 700 dbstat-version-bin<br />
  ./dbstat-version-bin/startup.sh

