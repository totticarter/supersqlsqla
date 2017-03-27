import jpype
import json


def jpypetest():
    jvmPath = jpype.getDefaultJVMPath()
    jpype.startJVM(jvmPath)
    jpype.java.lang.System.out.println("hello world!")
    jpype.shutdownJVM()

def extJarTest():
    jars = ["/Users/waixingren/software/sqlalchemy/fastjson-1.2.30.jar"]
    jvm_path = jpype.getDefaultJVMPath()
    jvm_cp = "-Djava.class.path={}".format(":".join(jars))

    jpype.startJVM(jvm_path, jvm_cp)

    JSONObject = jpype.JClass("com.alibaba.fastjson.JSONObject")
    json_str = json.dumps({"name": "yetship", "site": "https://liuliqiang.info"})
    jsonObj = JSONObject.parse(json_str)
    print(jsonObj.getString("name"))
    print(jsonObj.getString("site"))

    jpype.shutdownJVM()

def hiveJdbcTest():

    hivejdbcJars = ["/Users/waixingren/software/tencent/uaejdbc/supersql-jdbc/target/uaejdbc-1.0-SNAPSHOT-jar-with-dependencies.jar"]
    jvm_path = jpype.getDefaultJVMPath()
    jvm_cp = "-Djava.class.path={}".format(":".join(hivejdbcJars))
    jpype.startJVM(jvm_path, jvm_cp)

    javaClass = jpype.JClass('org.apache.hive.jdbc.HiveDriver')
    url = "jdbc:hive2://localhost:10000/default"
    connection = jpype.java.sql.DriverManager.getConnection(url, "waixingren", "")
    print "connect to hive success!"

    connection.setSchema("tpchorc")
    statement = connection.createStatement();
    resultSet = statement.executeQuery("show tables")
    resultSet.next()
    print resultSet.getString("tab_name")

    # con.setSchema("tpchorc");
    # Statement
    # statement = con.createStatement();
    # ResultSet
    # resultSet = statement.executeQuery("show tables");
    # resultSet.next();
    # System.out.println(resultSet.getString("tab_name"));