package com.hylamobile.testcontainers;

import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PGTest {

    private static final String POSTGRES_TEST_IMAGE = "postgres:10.14";

    @Test
    public void testSimple() throws SQLException {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(POSTGRES_TEST_IMAGE)) {
            postgres.start();

            String query = "SELECT 1";
            performQuery(postgres, query, resultSet -> {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
            });
        }
    }

    @Test
    public void testJsonb() throws SQLException {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(POSTGRES_TEST_IMAGE)
                .withInitScript("init_json.sql")) {

            postgres.start();

            String query = "SELECT data->>'title' FROM books WHERE data->>'author' = 'James Joyce'";
            performQuery(postgres, query, resultSet -> {
                assertTrue(resultSet.next());
                assertEquals("Ulysses", resultSet.getString(1));
            });
        }
    }

    @Test
    public void testTsvector() throws SQLException {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(POSTGRES_TEST_IMAGE)
                .withInitScript("init_tsvector.sql")) {

            postgres.start();

            {
                String query = "SELECT count(*) FROM words WHERE data @@ to_tsquery('get & done')";
                performQuery(postgres, query, resultSet -> {
                    assertTrue(resultSet.next());
                    assertTrue(resultSet.getBoolean(1));
                });
            }

            {
                String query = "SELECT count(*) FROM words WHERE data @@ to_tsquery('get & let')";
                performQuery(postgres, query, resultSet -> {
                    assertTrue(resultSet.next());
                    assertFalse(resultSet.getBoolean(1));
                });
            }
        }
    }

    @Test
    public void testLtree() throws SQLException {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(POSTGRES_TEST_IMAGE)
                .withInitScript("init_ltree.sql")) {

            postgres.start();

            {
                String query = "SELECT path FROM test WHERE path <@ 'Top.Science'";
                performQuery(postgres, query, resultSet -> {
                    List<String> paths = toStringList(resultSet);
                    assertThat(paths, hasItems(
                            "Top.Science",
                            "Top.Science.Astronomy",
                            "Top.Science.Astronomy.Astrophysics",
                            "Top.Science.Astronomy.Cosmology"));
                });
            }

            {
                String query = "SELECT path FROM test WHERE path ~ '*.!pictures@.*.Astronomy.*';";
                performQuery(postgres, query, resultSet -> {
                    List<String> paths = toStringList(resultSet);
                    assertThat(paths, hasItems(
                            "Top.Science.Astronomy",
                            "Top.Science.Astronomy.Astrophysics",
                            "Top.Science.Astronomy.Cosmology"));
                });
            }
        }
    }

    @Test
    public void testHstore() throws SQLException {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(POSTGRES_TEST_IMAGE)
                .withInitScript("init_hstore.sql")) {

            postgres.start();

            {
                String query = "SELECT data FROM dict";
                performQuery(postgres, query, resultSet -> {
                    assertTrue(resultSet.next());
                    @SuppressWarnings("unchecked")
                    Map<String, String> dict = (Map<String, String>) resultSet.getObject(1);
                    assertEquals(dict.get("a"), "1");
                    assertEquals(dict.get("b"), "2");
                    assertEquals(dict.get("c"), "3");
                });
            }

            {
                String query = "SELECT delete(data, 'c') FROM dict";
                performQuery(postgres, query, resultSet -> {
                    assertTrue(resultSet.next());
                    @SuppressWarnings("unchecked")
                    Map<String, String> dict = (Map<String, String>) resultSet.getObject(1);
                    assertEquals(dict.get("a"), "1");
                    assertEquals(dict.get("b"), "2");
                    assertFalse(dict.containsKey("c"));
                });
            }

            {
                String query = "SELECT data || hstore('d', '4') FROM dict";
                performQuery(postgres, query, resultSet -> {
                    assertTrue(resultSet.next());
                    @SuppressWarnings("unchecked")
                    Map<String, String> dict = (Map<String, String>) resultSet.getObject(1);
                    assertEquals(dict.get("a"), "1");
                    assertEquals(dict.get("b"), "2");
                    assertEquals(dict.get("c"), "3");
                    assertEquals(dict.get("d"), "4");
                });
            }
        }
    }

    private void performQuery(JdbcDatabaseContainer<?> container, String query, ResultSetConsumer consumer) throws SQLException {
        try (
            Connection conn = container.createConnection("");
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet resultSet = stmt.executeQuery()) {

            consumer.accept(resultSet);
        }
    }

    private List<String> toStringList(ResultSet resultSet) throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString(1));
        }
        return result;
    }

    interface ResultSetConsumer {
        void accept(ResultSet resultSet) throws SQLException;
    }
}
