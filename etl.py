<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
 <name>javax.jdo.option.ConnectionURL</name>
 <value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true</value>
 <description>metadata is stored in a MySQL server</description>
</property>
<property>
 <name>javax.jdo.option.ConnectionDriverName</name>
 <value>com.mysql.jdbc.Driver</value>
 <description>MySQL JDBC driver class</description>
</property>
<property>
 <name>javax.jdo.option.ConnectionUserName</name>
 <value>hive</value>
 <description>user name for connecting to mysql server </description>
</property>
<property>
 <name>javax.jdo.option.ConnectionPassword</name>
 <value>123456</value>
 <description>password for connecting to mysql server </description>
</property>
</configuration>