import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


public class ConnectionFactoryMy {
    public static Connection createConnection(){
        Configuration conf = HBaseConfiguration.create();

        Connection conn = null;

        // 建立一个数据库的连接
        try {
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }

        return conn;
    }
}
