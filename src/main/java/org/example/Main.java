package org.example;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

import java.util.UUID;

public class Main {

    private static final String contactPoints = "max-dev";


    public static void main(String[] args) {
        try (Cluster cluster = connect(contactPoints)) {
            Session session = cluster.connect();
            session.execute("DROP KEYSPACE IF EXISTS lwt_test");
            session.execute("CREATE KEYSPACE lwt_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES = true");
            session.execute("CREATE TABLE lwt_test.lwt (key timeuuid, dummy text, value text, PRIMARY KEY(key))");
            session.close();

            session = cluster.connect("lwt_test");

            UUID key = UUIDs.timeBased();
            String dummy = "ABC";
            String value = "DEF";
            session.execute("insert into lwt(key, dummy, value) values(?, ?, ?)", key, dummy, value);

            String value2 = "XYZ";
            ResultSet rs = session.execute("update lwt set value=? where key=?", value2, key);
            if (rs.wasApplied()) {
                rs = session.execute("update lwt set value=null where key=? if value=?", key, value2);
                if (rs.wasApplied()) {
                    Row row = session.execute("select value from lwt where key=?", key).one();
                    String actualValue = row.getString("value");
                    if (actualValue != null) {
                        throw new RuntimeException("Should be null but was " + actualValue);
                    }
                }
            }
        }
    }

    private static Cluster connect(String contactPoints) {
        return Cluster.builder()
                .addContactPoints(contactPoints)
                .withPort(9042)
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(90000)
                        .setReadTimeoutMillis(90000)
                )
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
//                .withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                .build();
    }

}