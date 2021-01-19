package software.amazon.documentdb;

import javax.sql.PooledConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * DocumentDb implementation of DataSource.
 */
public class DocumentDbDataSource extends software.amazon.jdbc.DataSource implements javax.sql.DataSource, javax.sql.ConnectionPoolDataSource {
    private final DocumentDBConnectionProperties properties;

    /**
     * DocumentDbDataSource constructor, initializes super class.
     * @param properties Properties Object.
     */
    DocumentDbDataSource(final DocumentDBConnectionProperties properties) {
        super();
        this.properties = (DocumentDBConnectionProperties) properties.clone();
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        return new DocumentDbConnection(properties);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        // TODO: Add some auth logic.
        return null;
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return new DocumentDbPooledConnection(getConnection());
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new DocumentDbPooledConnection(getConnection(user, password));
    }

    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }
}
