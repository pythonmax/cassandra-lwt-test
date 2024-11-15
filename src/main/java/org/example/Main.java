package org.example;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

import java.util.UUID;

public class Main {

    private static final String contactPoints = "max-dev";


    public static void main(String[] args) throws Exception {
        try (Cluster cluster = connect(contactPoints)) {
            Session session = cluster.connect();
            session.execute("DROP KEYSPACE IF EXISTS lwt_test");
            session.execute("CREATE KEYSPACE lwt_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES = true");
            session.execute("CREATE TABLE lwt_test.lwt (key timeuuid, dummy text, value text, PRIMARY KEY(key))");
            session.close();

            session = cluster.connect("lwt_test");

            final UUID key = UUIDs.timeBased();
            final String dummy = "ABC";
            final String value = "DEF";
            session.execute("insert into lwt(key, dummy, value) values(?, ?, ?)", key, dummy, value);

            final String value2 = "XYZ";
            session.execute("update lwt set value=? where key=?", value2, key);
            String actualValue = session.execute("select value from lwt where key=?", key).one().getString("value");
            if (!value2.equals(actualValue)) {
                throw new RuntimeException("Should be " + value + " but was " + actualValue);
            }

            // uncomment 'sleep' to make the problem go away
//            Thread.sleep(1000);

            ResultSet rs = session.execute("update lwt set value=null where key=? if value=?", key, value2);
            if (rs.wasApplied()) {
                actualValue = session.execute("select value from lwt where key=?", key).one().getString("value");
                if (actualValue != null) {
                    throw new RuntimeException("Should be null but was " + actualValue);
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