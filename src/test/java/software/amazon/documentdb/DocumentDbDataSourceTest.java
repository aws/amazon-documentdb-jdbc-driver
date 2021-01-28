package software.amazon.documentdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

/**
 * Tests for the DocumentDbDataSource
 */
public class DocumentDbDataSourceTest extends DocumentDbTest {

    private static DocumentDbDataSource dataSource;

    /**
     * Instantiates data source object for testing.
     */
    @BeforeEach
    public void initialize() {
        dataSource = new DocumentDbDataSource();
    }

    /**
     * Tests and validates with valid properties.
     *
     * @throws SQLException on invalid validation of properties.
     */
    @Test
    public void testValidProperties() throws SQLException {
        dataSource.setUser("username");
        dataSource.setPassword("password");
        dataSource.setDatabase("testDb");
        dataSource.setHostname("host");
        dataSource.setReplicaSet("rs0");
        dataSource.setReadPreference(DocumentDbReadPreference.PRIMARY);
        dataSource.setApplicationName("appName");
        dataSource.setTlsEnabled(false);
        dataSource.setTlsAllowInvalidHostnames(false);
        dataSource.setLoginTimeout(5);
        dataSource.setRetryReadsEnabled(false);
        dataSource.validateRequiredProperties(); // Will throw SQL exception if invalid
        Assertions.assertEquals("username", dataSource.getUser());
        Assertions.assertEquals("password", dataSource.getPassword());
        Assertions.assertEquals("testDb", dataSource.getDatabase());
        Assertions.assertEquals("host", dataSource.getHostname());
        Assertions.assertEquals("rs0", dataSource.getReplicaSet());
        Assertions.assertEquals(DocumentDbReadPreference.PRIMARY, dataSource.getReadPreference());
        Assertions.assertEquals("appName", dataSource.getApplicationName());
        Assertions.assertFalse(dataSource.getTlsEnabled());
        Assertions.assertFalse(dataSource.getTlsAllowInvalidHosts());
        Assertions.assertEquals(5L, dataSource.getLoginTimeout());
        Assertions.assertFalse(dataSource.getRetryReadsEnabled());
    }

    /**
     * Tests invalid property settings.
     */
    @Test
    public void testInvalidPropertySettings() throws SQLException {
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.setLoginTimeout(-1));
    }

    /**
     * Tests required properties validation with invalid inputs.
     */
    @Test
    public void testMissingPropertiesValidation() {
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setUser("");
        dataSource.setPassword("password");
        dataSource.setDatabase("db");
        dataSource.setHostname("validHost");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setUser("user");
        dataSource.setPassword(" ");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setPassword("password");
        dataSource.setDatabase("    ");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setDatabase("database");
        dataSource.setHostname("");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
    }

}
