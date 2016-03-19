create ycsb database, usertable table inside with data jsonb field

# PG

./bin/ycsb load pgjsonb -s -P workloads/workloada -p recordcount=100 -threads 2 -cp jdbc-binding/lib/postgresql-9.4.1207.jar -p db.driver=org.postgresql.Driver -p db.url=jdbc:postgresql://localhost:5432/ycsb -p db.user=erthalion
./bin/ycsb run pgjsonb -s -P workloads/workloada -p recordcount=100 -threads 2 -cp jdbc-binding/lib/postgresql-9.4.1207.jar -p db.driver=org.postgresql.Driver -p db.url=jdbc:postgresql://localhost:5432/ycsb -p db.user=erthalion

# Mysql

./bin/ycsb load mysqljson -s -P workloads/workloada -p recordcount=100 -threads 2 -cp jdbc-binding/lib/mysql-connector-java-5.1.38-bin.jar -p db.driver=com.mysql.jdbc.Driver -p db.url=jdbc:mysql://localhost:3306/ycsb -p db.user=root -p db.passwd=123456
./bin/ycsb run mysqljson -s -P workloads/workloada -p recordcount=100 -threads 2 -cp jdbc-binding/lib/mysql-connector-java-5.1.38-bin.jar -p db.driver=com.mysql.jdbc.Driver -p db.url=jdbc:mysql://localhost:3306/ycsb -p db.user=root -p db.passwd=123456


# Mongodb

/bin/ycsb load mongodb -s -P workloads/workloada -p recordcount=1000000 -threads 8 -p mongodb.url="mongodb://mongo:mongo@52.25.20.8:27017/ycsb" -p mongodb.auth="true"
./bin/ycsb run mongodb -s -P workloads/workloada -p recordcount=100 -threads 2 -p mongodb.url="mongodb://mongo:mongo@52.25.20.8:27017/ycsb" -p mongodb.auth="true"
