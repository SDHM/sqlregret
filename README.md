###sql后悔药使用手册

####配置文件说明
<pre>
{
  "masterPosition" : 4,
  "mode" : "online",
  "basePath":"/usr/local/mysql/data",
  "indexFile":"mysql-bin.index",
  "masterJournalName" : "mysql-bin.000001",
  "masterPort" : 3306,
  "masterAddress" : "127.0.0.1",
  "slaveId" : 5,
  "dbPassword" : "123456",
  "defaultDbName" : "app",
  "dbUsername" : "reader"
}
</pre>

1. masterPosition 

    开始解析的位置 在onfile模式下无意义，并控制不了
    
    因为文件头有重要信息

2. mode 

    online 在线模式
    
    onfile 文件模式

3. basePath 

    文件模式下的日志路径
    
4. indexFile
    
    文件模式下binlog日志文件索引文件

5. masterJournalName
    
    起始日志文件文件名

6. masterPort
    
    master数据库端口(相对于sqlregret来说)

7. masterAddress
    
    master数据库地址(相对于sqlregret来说)

8. slaveId
    
    sqlregret模拟的从库id(online模式下)

9. dbPassword 

    账号密码(onfile模式下也需要连接数据库，读取数据类型、主键等元信息)
    
10. defaultDbName 
    
    默认数据库(use database)

11. dbUsername

    账号名称
  
####运行模式

`通过sqlregret.conf 中的mode参数控制`
1. 在线模式 `online`
   
    通过模拟slave从主库dump日志，并解析

2. 读文件模式 `onfile`
    
    直接读取数据库日志文件进行解析(可减轻数据库负担，防止因日志同步协议升级导致无法进行日志同步)

####解析模式
`通过命令行参数--mode控制 可选值为parse、mark`
1. 解析增、删、改操作   `parse`

2. 解析binlog时间位置  `mark`

   平时运行在这个模式下，便于要解析的时候快速定位文件和位置

####解析范围控制
1. 时间控制  
    `通过命令行参数 --start-time --end-time 控制`

    可以同时指定，可以同时为空，可以只指定`start-time`，但是不能只指定`end-time`
    
2. 文件以及位置控制
    `通过命令行参数 --start-file --start-pos  --end-file --end-pos控制`
    
    可以只指定start, 不指定end、也可以同时指定、也可以同时为空，但不能只有end没有start

####解析类型控制

`通过命令行参数--filter-sql指定`

1. update

    只解析update
    
2. insert
    
    只解析insert

3. delete

    只解析delete


####输出语句控制
`通过命令行参数--rsv控制`

1. true
    
    输出反向语句

2. false
    
    不输出反向语句


####DDL语句输出控制
`通过命令行参数--with-ddl控制`

1. true
    
    输出DDL语句

2. false
    
    不输出DDL语句

####解析目标控制

1. 指定解析数据库

    `通过--filter-db控制，指定要解析的数据库名`
    
2. 指定解析数据表

    `通过--filter-table控制，指定要解析的数据表名`
    
####字段控制


####字段前后值控制

delete 控制这个没有意义

insert 字段|改动后

update 字段|改动前|改动后,字段|改动前|改动后

















