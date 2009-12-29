/*
 * JBoss DNA (http://www.jboss.org/dna)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of 
 * individual contributors. 
 *
 * JBoss DNA is free software. Unless otherwise indicated, all code in JBoss DNA
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * JBoss DNA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.dna.connector.meta.jdbc;

import java.beans.PropertyVetoException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;
import net.jcip.annotations.ThreadSafe;
import org.jboss.dna.common.i18n.I18n;
import org.jboss.dna.common.util.Logger;
import org.jboss.dna.graph.ExecutionContext;
import org.jboss.dna.graph.cache.CachePolicy;
import org.jboss.dna.graph.connector.RepositoryConnection;
import org.jboss.dna.graph.connector.RepositoryContext;
import org.jboss.dna.graph.connector.RepositorySourceCapabilities;
import org.jboss.dna.graph.connector.RepositorySourceException;
import org.jboss.dna.graph.connector.path.PathRepositoryConnection;
import org.jboss.dna.graph.connector.path.PathRepositorySource;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@ThreadSafe
public class JdbcMetadataSource implements PathRepositorySource, ObjectFactory {

    private static final long serialVersionUID = 1L;

    private final Logger log = Logger.getLogger(JdbcMetadataSource.class);

    protected static final String SOURCE_NAME = "sourceName";
    protected static final String ROOT_NODE_UUID = "rootNodeUuid";
    protected static final String DATA_SOURCE_JNDI_NAME = "dataSourceJndiName";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    protected static final String URL = "url";
    protected static final String DRIVER_CLASS_NAME = "driverClassName";
    protected static final String DRIVER_CLASSLOADER_NAME = "driverClassloaderName";
    protected static final String MAXIMUM_CONNECTIONS_IN_POOL = "maximumConnectionsInPool";
    protected static final String MINIMUM_CONNECTIONS_IN_POOL = "minimumConnectionsInPool";
    protected static final String MAXIMUM_CONNECTION_IDLE_TIME_IN_SECONDS = "maximumConnectionIdleTimeInSeconds";
    protected static final String MAXIMUM_SIZE_OF_STATEMENT_CACHE = "maximumSizeOfStatementCache";
    protected static final String NUMBER_OF_CONNECTIONS_TO_BE_ACQUIRED_AS_NEEDED = "numberOfConnectionsToBeAcquiredAsNeeded";
    protected static final String IDLE_TIME_IN_SECONDS_BEFORE_TESTING_CONNECTIONS = "idleTimeInSecondsBeforeTestingConnections";
    protected static final String CACHE_TIME_TO_LIVE_IN_MILLISECONDS = "cacheTimeToLiveInMilliseconds";
    protected static final String RETRY_LIMIT = "retryLimit";
    protected static final String DEFAULT_WORKSPACE = "defaultWorkspace";
    protected static final String DEFAULT_CATALOG_NAME = "defaultCatalogName";
    protected static final String DEFAULT_SCHEMA_NAME = "defaultSchemaName";
    protected static final String METADATA_COLLECTOR_CLASS_NAME = "metadataCollectorClassName";

    /**
     * This source does not support events.
     */
    protected static final boolean SUPPORTS_EVENTS = false;
    /**
     * This source does support same-name-siblings for procedure nodes.
     */
    protected static final boolean SUPPORTS_SAME_NAME_SIBLINGS = true;
    /**
     * This source does not support creating references.
     */
    protected static final boolean SUPPORTS_REFERENCES = false;
    /**
     * This source does not support updates.
     */
    public static final boolean SUPPORTS_UPDATES = false;
    /**
     * This source does not support creating workspaces.
     */
    public static final boolean SUPPORTS_CREATING_WORKSPACES = false;

    /**
     * The default UUID that is used for root nodes in a store.
     */
    public static final String DEFAULT_ROOT_NODE_UUID = "cafebabe-cafe-babe-cafe-babecafebabe";

    /**
     * The initial {@link #getDefaultWorkspaceName() name of the default workspace} is "{@value} ", unless otherwise specified.
     */
    public static final String DEFAULT_NAME_OF_DEFAULT_WORKSPACE = "default";

    /**
     * The initial {@link #getDefaultCatalogName() catalog name for databases that do not support catalogs} is "{@value} ", unless
     * otherwise specified.
     */
    public static final String DEFAULT_NAME_OF_DEFAULT_CATALOG = "default";

    /**
     * The initial {@link #getDefaultSchemaName() schema name for databases that do not support schemas} is "{@value} ", unless
     * otherwise specified.
     */
    public static final String DEFAULT_NAME_OF_DEFAULT_SCHEMA = "default";

    private static final int DEFAULT_RETRY_LIMIT = 0;
    private static final int DEFAULT_CACHE_TIME_TO_LIVE_IN_SECONDS = 60 * 5; // 5 minutes
    private static final int DEFAULT_MAXIMUM_CONNECTIONS_IN_POOL = 5;
    private static final int DEFAULT_MINIMUM_CONNECTIONS_IN_POOL = 0;
    private static final int DEFAULT_MAXIMUM_CONNECTION_IDLE_TIME_IN_SECONDS = 60 * 10; // 10 minutes
    private static final int DEFAULT_MAXIMUM_NUMBER_OF_STATEMENTS_TO_CACHE = 100;
    private static final int DEFAULT_NUMBER_OF_CONNECTIONS_TO_ACQUIRE_AS_NEEDED = 1;
    private static final int DEFAULT_IDLE_TIME_IN_SECONDS_BEFORE_TESTING_CONNECTIONS = 60 * 3; // 3 minutes
    private static final MetadataCollector DEFAULT_METADATA_COLLECTOR = new JdbcMetadataCollector();

    private volatile String name;
    private volatile String dataSourceJndiName;
    private volatile String username;
    private volatile String password;
    private volatile String url;
    private volatile String driverClassName;
    private volatile String driverClassloaderName;
    private volatile String rootNodeUuid = DEFAULT_ROOT_NODE_UUID;
    private volatile int maximumConnectionsInPool = DEFAULT_MAXIMUM_CONNECTIONS_IN_POOL;
    private volatile int minimumConnectionsInPool = DEFAULT_MINIMUM_CONNECTIONS_IN_POOL;
    private volatile int maximumConnectionIdleTimeInSeconds = DEFAULT_MAXIMUM_CONNECTION_IDLE_TIME_IN_SECONDS;
    private volatile int maximumSizeOfStatementCache = DEFAULT_MAXIMUM_NUMBER_OF_STATEMENTS_TO_CACHE;
    private volatile int numberOfConnectionsToAcquireAsNeeded = DEFAULT_NUMBER_OF_CONNECTIONS_TO_ACQUIRE_AS_NEEDED;
    private volatile int idleTimeInSecondsBeforeTestingConnections = DEFAULT_IDLE_TIME_IN_SECONDS_BEFORE_TESTING_CONNECTIONS;
    private volatile int retryLimit = DEFAULT_RETRY_LIMIT;
    private volatile int cacheTimeToLiveInMilliseconds = DEFAULT_CACHE_TIME_TO_LIVE_IN_SECONDS * 1000;
    private volatile String defaultWorkspace = DEFAULT_NAME_OF_DEFAULT_WORKSPACE;
    private volatile String defaultCatalogName = DEFAULT_NAME_OF_DEFAULT_CATALOG;
    private volatile String defaultSchemaName = DEFAULT_NAME_OF_DEFAULT_SCHEMA;
    private volatile String metadataCollectorClassName = DEFAULT_METADATA_COLLECTOR.getClass().getName();

    private volatile RepositorySourceCapabilities capabilities = new RepositorySourceCapabilities(SUPPORTS_SAME_NAME_SIBLINGS,
                                                                                                  SUPPORTS_UPDATES,
                                                                                                  SUPPORTS_EVENTS,
                                                                                                  SUPPORTS_CREATING_WORKSPACES,
                                                                                                  SUPPORTS_REFERENCES);
    private transient DataSource dataSource;
    private transient CachePolicy cachePolicy;
    private transient JdbcMetadataRepository repository;
    private transient RepositoryContext repositoryContext;
    private transient UUID rootUuid = UUID.fromString(rootNodeUuid);
    private transient MetadataCollector metadataCollector = DEFAULT_METADATA_COLLECTOR;

    final JdbcMetadataRepository repository() {
        return this.repository;
    }

    /**
     * Set the time in milliseconds that content returned from this source may used while in the cache.
     * 
     * @param cacheTimeToLive the time to live, in milliseconds; 0 if the time to live is not specified by this source; or a
     *        negative number for the default value
     */
    public synchronized void setCacheTimeToLiveInMilliseconds( int cacheTimeToLive ) {
        if (cacheTimeToLive < 0) cacheTimeToLive = DEFAULT_CACHE_TIME_TO_LIVE_IN_SECONDS;
        this.cacheTimeToLiveInMilliseconds = cacheTimeToLive;
        if (this.cacheTimeToLiveInMilliseconds == 0) {
            this.cachePolicy = null;
        } else {
            final int ttl = this.cacheTimeToLiveInMilliseconds;
            this.cachePolicy = new CachePolicy() {

                private static final long serialVersionUID = 1L;

                public long getTimeToLive() {
                    return ttl;
                }
            };
        }
    }

    public long getCacheTimeToLiveInMilliseconds() {
        return this.cacheTimeToLiveInMilliseconds;
    }

    public CachePolicy getDefaultCachePolicy() {
        return this.cachePolicy;
    }

    public RepositoryContext getRepositoryContext() {
        return this.repositoryContext;
    }

    public void close() {
        if (this.dataSource instanceof ComboPooledDataSource) {
            ((ComboPooledDataSource)this.dataSource).close();
        }
    }

    /**
     * @return the datasource that corresponds to the login information provided to this source
     * @see #setUsername(String)
     * @see #setPassword(String)
     * @see #setDriverClassName(String)
     * @see #setDriverClassloaderName(String)
     * @see #setUrl(String)
     * @see #setDataSourceJndiName(String)
     */
    DataSource getDataSource() {
        if (this.dataSource == null) {
            loadDataSource();
        }
        return this.dataSource;
    }

    public RepositorySourceCapabilities getCapabilities() {
        return this.capabilities;
    }

    /*
     * Synchronized to avoid race conditions with setters of datasource-related properties
     */
    private synchronized void loadDataSource() throws RepositorySourceException {
        // Now set the mandatory information, overwriting anything that the subclasses may have tried ...
        if (this.dataSource == null && this.dataSourceJndiName != null) {
            // Try to load the DataSource from JNDI ...
            try {
                Context context = new InitialContext();
                dataSource = (DataSource)context.lookup(this.dataSourceJndiName);
            } catch (Throwable t) {
                log.error(t, JdbcMetadataI18n.errorFindingDataSourceInJndi, name, dataSourceJndiName);
            }
        }

        if (this.dataSource == null) {
            // Set the context class loader, so that the driver could be found ...
            if (this.repositoryContext != null && this.driverClassloaderName != null) {
                try {
                    ExecutionContext context = this.repositoryContext.getExecutionContext();
                    ClassLoader loader = context.getClassLoader(this.driverClassloaderName);
                    if (loader != null) {
                        Thread.currentThread().setContextClassLoader(loader);
                    }
                } catch (Throwable t) {
                    I18n msg = JdbcMetadataI18n.errorSettingContextClassLoader;
                    log.error(t, msg, name, driverClassloaderName);
                }
            }

            if (this.driverClassName == null || this.url == null) {
                throw new RepositorySourceException(JdbcMetadataI18n.driverClassNameAndUrlAreRequired.text(driverClassName, url));
            }

            ComboPooledDataSource cpds = new ComboPooledDataSource();

            try {
                cpds.setDriverClass(this.driverClassName);
                cpds.setJdbcUrl(this.url);
                cpds.setUser(this.username);
                cpds.setPassword(this.password);
                cpds.setMaxStatements(this.maximumSizeOfStatementCache);
                cpds.setAcquireRetryAttempts(retryLimit);
                cpds.setMaxIdleTime(this.maximumConnectionIdleTimeInSeconds);
                cpds.setMinPoolSize(this.minimumConnectionsInPool);
                cpds.setMaxPoolSize(this.maximumConnectionsInPool);
                cpds.setAcquireIncrement(this.numberOfConnectionsToAcquireAsNeeded);
                cpds.setIdleConnectionTestPeriod(this.idleTimeInSecondsBeforeTestingConnections);

            } catch (PropertyVetoException pve) {
                throw new IllegalStateException(JdbcMetadataI18n.couldNotSetDriverProperties.text(), pve);
            }

            this.dataSource = cpds;
        }

    }

    public RepositoryConnection getConnection() throws RepositorySourceException {
        if (this.name == null || this.name.trim().length() == 0) {
            throw new RepositorySourceException(JdbcMetadataI18n.repositorySourceMustHaveName.text());
        }

        if (repository == null) {
            repository = new JdbcMetadataRepository(this);
        }

        return new PathRepositoryConnection(this, repository);
    }

    /*
     * (non-Javadoc)
     * @see org.jboss.dna.graph.connector.RepositorySource#getName()
     */
    public String getName() {
        return this.name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * @see org.jboss.dna.graph.connector.RepositorySource#getRetryLimit()
     */
    public int getRetryLimit() {
        return this.retryLimit;
    }

    /*
     * (non-Javadoc)
     * @see org.jboss.dna.graph.connector.RepositorySource#initialize(org.jboss.dna.graph.connector.RepositoryContext)
     */
    public void initialize( RepositoryContext context ) throws RepositorySourceException {
        this.repositoryContext = context;

    }

    /*
     * (non-Javadoc)
     * @see org.jboss.dna.graph.connector.RepositorySource#setRetryLimit(int)
     */
    public void setRetryLimit( int limit ) {
        this.retryLimit = limit;

    }

    public Reference getReference() {
        String className = getClass().getName();
        String factoryClassName = this.getClass().getName();
        Reference ref = new Reference(className, factoryClassName, null);

        ref.add(new StringRefAddr(SOURCE_NAME, getName()));
        ref.add(new StringRefAddr(ROOT_NODE_UUID, getRootNodeUuid()));
        ref.add(new StringRefAddr(DATA_SOURCE_JNDI_NAME, getDataSourceJndiName()));
        ref.add(new StringRefAddr(USERNAME, getUsername()));
        ref.add(new StringRefAddr(PASSWORD, getPassword()));
        ref.add(new StringRefAddr(URL, getUrl()));
        ref.add(new StringRefAddr(DRIVER_CLASS_NAME, getDriverClassName()));
        ref.add(new StringRefAddr(DRIVER_CLASSLOADER_NAME, getDriverClassloaderName()));
        ref.add(new StringRefAddr(MAXIMUM_CONNECTIONS_IN_POOL, Integer.toString(getMaximumConnectionsInPool())));
        ref.add(new StringRefAddr(MINIMUM_CONNECTIONS_IN_POOL, Integer.toString(getMinimumConnectionsInPool())));
        ref.add(new StringRefAddr(MAXIMUM_CONNECTION_IDLE_TIME_IN_SECONDS,
                                  Integer.toString(getMaximumConnectionIdleTimeInSeconds())));
        ref.add(new StringRefAddr(MAXIMUM_SIZE_OF_STATEMENT_CACHE, Integer.toString(getMaximumSizeOfStatementCache())));
        ref.add(new StringRefAddr(NUMBER_OF_CONNECTIONS_TO_BE_ACQUIRED_AS_NEEDED,
                                  Integer.toString(getNumberOfConnectionsToAcquireAsNeeded())));
        ref.add(new StringRefAddr(IDLE_TIME_IN_SECONDS_BEFORE_TESTING_CONNECTIONS,
                                  Integer.toString(getIdleTimeInSecondsBeforeTestingConnections())));
        ref.add(new StringRefAddr(CACHE_TIME_TO_LIVE_IN_MILLISECONDS, Long.toString(getCacheTimeToLiveInMilliseconds())));
        ref.add(new StringRefAddr(DEFAULT_WORKSPACE, getDefaultWorkspaceName()));
        ref.add(new StringRefAddr(RETRY_LIMIT, Integer.toString(getRetryLimit())));

        ref.add(new StringRefAddr(DEFAULT_CATALOG_NAME, getDefaultCatalogName()));
        ref.add(new StringRefAddr(DEFAULT_SCHEMA_NAME, getDefaultSchemaName()));
        ref.add(new StringRefAddr(METADATA_COLLECTOR_CLASS_NAME, getMetadataCollectorClassName()));

        return ref;
    }

    public Object getObjectInstance( Object obj,
                                     javax.naming.Name name,
                                     Context nameCtx,
                                     Hashtable<?, ?> environment ) throws Exception {
        if (!(obj instanceof Reference)) {
            return null;
        }

        Map<String, String> values = new HashMap<String, String>();
        Reference ref = (Reference)obj;
        Enumeration<?> en = ref.getAll();
        while (en.hasMoreElements()) {
            RefAddr subref = (RefAddr)en.nextElement();
            if (subref instanceof StringRefAddr) {
                String key = subref.getType();
                Object value = subref.getContent();
                if (value != null) values.put(key, value.toString());
            }
        }
        String sourceName = values.get(SOURCE_NAME);
        String rootNodeUuid = values.get(ROOT_NODE_UUID);
        String dataSourceJndiName = values.get(DATA_SOURCE_JNDI_NAME);
        String username = values.get(USERNAME);
        String password = values.get(PASSWORD);
        String url = values.get(URL);
        String driverClassName = values.get(DRIVER_CLASS_NAME);
        String driverClassloaderName = values.get(DRIVER_CLASSLOADER_NAME);
        String maxConnectionsInPool = values.get(MAXIMUM_CONNECTIONS_IN_POOL);
        String minConnectionsInPool = values.get(MINIMUM_CONNECTIONS_IN_POOL);
        String maxConnectionIdleTimeInSec = values.get(MAXIMUM_CONNECTION_IDLE_TIME_IN_SECONDS);
        String maxSizeOfStatementCache = values.get(MAXIMUM_SIZE_OF_STATEMENT_CACHE);
        String acquisitionIncrement = values.get(NUMBER_OF_CONNECTIONS_TO_BE_ACQUIRED_AS_NEEDED);
        String idleTimeInSeconds = values.get(IDLE_TIME_IN_SECONDS_BEFORE_TESTING_CONNECTIONS);
        String cacheTtlInMillis = values.get(CACHE_TIME_TO_LIVE_IN_MILLISECONDS);
        String retryLimit = values.get(RETRY_LIMIT);
        String defaultWorkspace = values.get(DEFAULT_WORKSPACE);
        String defaultCatalogName = values.get(DEFAULT_CATALOG_NAME);
        String defaultSchemaName = values.get(DEFAULT_SCHEMA_NAME);
        String metadataCollectorClassName = values.get(METADATA_COLLECTOR_CLASS_NAME);

        // Create the source instance ...
        JdbcMetadataSource source = new JdbcMetadataSource();
        if (sourceName != null) source.setName(sourceName);
        if (rootNodeUuid != null) source.setRootNodeUuid(rootNodeUuid);
        if (dataSourceJndiName != null) source.setDataSourceJndiName(dataSourceJndiName);
        if (username != null) source.setUsername(username);
        if (password != null) source.setPassword(password);
        if (url != null) source.setUrl(url);
        if (driverClassName != null) source.setDriverClassName(driverClassName);
        if (driverClassloaderName != null) source.setDriverClassloaderName(driverClassloaderName);
        if (maxConnectionsInPool != null) source.setMaximumConnectionsInPool(Integer.parseInt(maxConnectionsInPool));
        if (minConnectionsInPool != null) source.setMinimumConnectionsInPool(Integer.parseInt(minConnectionsInPool));
        if (maxConnectionIdleTimeInSec != null) source.setMaximumConnectionIdleTimeInSeconds(Integer.parseInt(maxConnectionIdleTimeInSec));
        if (maxSizeOfStatementCache != null) source.setMaximumSizeOfStatementCache(Integer.parseInt(maxSizeOfStatementCache));
        if (acquisitionIncrement != null) source.setNumberOfConnectionsToAcquireAsNeeded(Integer.parseInt(acquisitionIncrement));
        if (idleTimeInSeconds != null) source.setIdleTimeInSecondsBeforeTestingConnections(Integer.parseInt(idleTimeInSeconds));
        if (cacheTtlInMillis != null) source.setCacheTimeToLiveInMilliseconds(Integer.parseInt(cacheTtlInMillis));
        if (retryLimit != null) source.setRetryLimit(Integer.parseInt(retryLimit));
        if (defaultWorkspace != null) source.setDefaultWorkspaceName(defaultWorkspace);
        if (defaultCatalogName != null) source.setDefaultCatalogName(defaultCatalogName);
        if (defaultSchemaName != null) source.setDefaultCatalogName(defaultSchemaName);
        if (metadataCollectorClassName != null) source.setMetadataCollectorClassName(metadataCollectorClassName);

        return source;

    }

    /**
     * @return dataSourceJndiName
     */
    public String getDataSourceJndiName() {
        return dataSourceJndiName;
    }

    /**
     * @param dataSourceJndiName Sets dataSourceJndiName to the specified value.
     */
    public void setDataSourceJndiName( String dataSourceJndiName ) {
        if (dataSourceJndiName != null && dataSourceJndiName.trim().length() == 0) dataSourceJndiName = null;
        this.dataSourceJndiName = dataSourceJndiName;
    }

    /**
     * @return driverClassName
     */
    public String getDriverClassName() {
        return driverClassName;
    }

    /**
     * @param driverClassName Sets driverClassName to the specified value.
     */
    public synchronized void setDriverClassName( String driverClassName ) {
        if (driverClassName != null && driverClassName.trim().length() == 0) driverClassName = null;
        this.driverClassName = driverClassName;
    }

    /**
     * @return driverClassloaderName
     */
    public String getDriverClassloaderName() {
        return driverClassloaderName;
    }

    /**
     * @param driverClassloaderName Sets driverClassloaderName to the specified value.
     */
    public synchronized void setDriverClassloaderName( String driverClassloaderName ) {
        if (driverClassloaderName != null && driverClassloaderName.trim().length() == 0) driverClassloaderName = null;
        this.driverClassloaderName = driverClassloaderName;
    }

    /**
     * @return username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username Sets username to the specified value.
     */
    public synchronized void setUsername( String username ) {
        this.username = username;
    }

    /**
     * @return password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password Sets password to the specified value.
     */
    public synchronized void setPassword( String password ) {
        this.password = password;
    }

    /**
     * @return url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @param url Sets url to the specified value.
     */
    public synchronized void setUrl( String url ) {
        if (url != null && url.trim().length() == 0) url = null;
        this.url = url;
    }

    /**
     * @return maximumConnectionsInPool
     */
    public int getMaximumConnectionsInPool() {
        return maximumConnectionsInPool;
    }

    /**
     * @param maximumConnectionsInPool Sets maximumConnectionsInPool to the specified value.
     */
    public synchronized void setMaximumConnectionsInPool( int maximumConnectionsInPool ) {
        if (maximumConnectionsInPool < 0) maximumConnectionsInPool = DEFAULT_MAXIMUM_CONNECTIONS_IN_POOL;
        this.maximumConnectionsInPool = maximumConnectionsInPool;
    }

    /**
     * @return minimumConnectionsInPool
     */
    public int getMinimumConnectionsInPool() {
        return minimumConnectionsInPool;
    }

    /**
     * @param minimumConnectionsInPool Sets minimumConnectionsInPool to the specified value.
     */
    public synchronized void setMinimumConnectionsInPool( int minimumConnectionsInPool ) {
        if (minimumConnectionsInPool < 0) minimumConnectionsInPool = DEFAULT_MINIMUM_CONNECTIONS_IN_POOL;
        this.minimumConnectionsInPool = minimumConnectionsInPool;
    }

    /**
     * @return maximumConnectionIdleTimeInSeconds
     */
    public int getMaximumConnectionIdleTimeInSeconds() {
        return maximumConnectionIdleTimeInSeconds;
    }

    /**
     * @param maximumConnectionIdleTimeInSeconds Sets maximumConnectionIdleTimeInSeconds to the specified value.
     */
    public synchronized void setMaximumConnectionIdleTimeInSeconds( int maximumConnectionIdleTimeInSeconds ) {
        if (maximumConnectionIdleTimeInSeconds < 0) maximumConnectionIdleTimeInSeconds = DEFAULT_MAXIMUM_CONNECTION_IDLE_TIME_IN_SECONDS;
        this.maximumConnectionIdleTimeInSeconds = maximumConnectionIdleTimeInSeconds;
    }

    /**
     * @return maximumSizeOfStatementCache
     */
    public int getMaximumSizeOfStatementCache() {
        return maximumSizeOfStatementCache;
    }

    /**
     * @param maximumSizeOfStatementCache Sets maximumSizeOfStatementCache to the specified value.
     */
    public synchronized void setMaximumSizeOfStatementCache( int maximumSizeOfStatementCache ) {
        if (maximumSizeOfStatementCache < 0) maximumSizeOfStatementCache = DEFAULT_MAXIMUM_NUMBER_OF_STATEMENTS_TO_CACHE;
        this.maximumSizeOfStatementCache = maximumSizeOfStatementCache;
    }

    /**
     * @return numberOfConnectionsToAcquireAsNeeded
     */
    public int getNumberOfConnectionsToAcquireAsNeeded() {
        return numberOfConnectionsToAcquireAsNeeded;
    }

    /**
     * @param numberOfConnectionsToAcquireAsNeeded Sets numberOfConnectionsToAcquireAsNeeded to the specified value.
     */
    public synchronized void setNumberOfConnectionsToAcquireAsNeeded( int numberOfConnectionsToAcquireAsNeeded ) {
        if (numberOfConnectionsToAcquireAsNeeded < 0) numberOfConnectionsToAcquireAsNeeded = DEFAULT_NUMBER_OF_CONNECTIONS_TO_ACQUIRE_AS_NEEDED;
        this.numberOfConnectionsToAcquireAsNeeded = numberOfConnectionsToAcquireAsNeeded;
    }

    /**
     * @return idleTimeInSecondsBeforeTestingConnections
     */
    public int getIdleTimeInSecondsBeforeTestingConnections() {
        return idleTimeInSecondsBeforeTestingConnections;
    }

    /**
     * @param idleTimeInSecondsBeforeTestingConnections Sets idleTimeInSecondsBeforeTestingConnections to the specified value.
     */
    public synchronized void setIdleTimeInSecondsBeforeTestingConnections( int idleTimeInSecondsBeforeTestingConnections ) {
        if (idleTimeInSecondsBeforeTestingConnections < 0) idleTimeInSecondsBeforeTestingConnections = DEFAULT_IDLE_TIME_IN_SECONDS_BEFORE_TESTING_CONNECTIONS;
        this.idleTimeInSecondsBeforeTestingConnections = idleTimeInSecondsBeforeTestingConnections;
    }

    /**
     * Set the {@link DataSource} instance that this source should use.
     * 
     * @param dataSource the data source; may be null
     * @see #getDataSource()
     * @see #setDataSourceJndiName(String)
     */
    /*package*/synchronized void setDataSource( DataSource dataSource ) {
        this.dataSource = dataSource;
    }

    /**
     * @return rootNodeUuid
     */
    public String getRootNodeUuid() {
        return rootNodeUuid;
    }

    /**
     * @return rootUuid
     */
    public UUID getRootUuid() {
        return rootUuid;
    }

    /**
     * @param rootNodeUuid Sets rootNodeUuid to the specified value.
     * @throws IllegalArgumentException if the string value cannot be converted to UUID
     */
    public void setRootNodeUuid( String rootNodeUuid ) {
        if (rootNodeUuid != null && rootNodeUuid.trim().length() == 0) rootNodeUuid = DEFAULT_ROOT_NODE_UUID;
        this.rootUuid = UUID.fromString(rootNodeUuid);
        this.rootNodeUuid = rootNodeUuid;
    }

    /**
     * Get the name of the default workspace.
     * 
     * @return the name of the workspace that should be used by default, or null if there is no default workspace
     */
    public String getDefaultWorkspaceName() {
        return defaultWorkspace;
    }

    /**
     * Set the name of the workspace that should be used when clients don't specify a workspace.
     * 
     * @param nameOfDefaultWorkspace the name of the workspace that should be used by default, or null if the
     *        {@link #DEFAULT_NAME_OF_DEFAULT_WORKSPACE default name} should be used
     */
    public synchronized void setDefaultWorkspaceName( String nameOfDefaultWorkspace ) {
        this.defaultWorkspace = nameOfDefaultWorkspace != null ? nameOfDefaultWorkspace : DEFAULT_NAME_OF_DEFAULT_WORKSPACE;
    }

    /**
     * Get the name of the default catalog.
     * 
     * @return the name that should be used as the catalog name when the database does not support catalogs
     */
    public String getDefaultCatalogName() {
        return defaultCatalogName;
    }

    /**
     * Set the name of the catalog that should be used when the database does not support catalogs.
     * 
     * @param defaultCatalogName the name that should be used as the catalog name by default, or null if the
     *        {@link #DEFAULT_NAME_OF_DEFAULT_CATALOG default name} should be used
     */
    public void setDefaultCatalogName( String defaultCatalogName ) {
        this.defaultCatalogName = defaultCatalogName == null ? DEFAULT_NAME_OF_DEFAULT_CATALOG : defaultCatalogName;
    }

    /**
     * Get the name of the default schema.
     * 
     * @return the name that should be used as the schema name when the database does not support schemas
     */
    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    /**
     * Set the name of the schema that should be used when the database does not support schemas.
     * 
     * @param defaultSchemaName the name that should be used as the schema name by default, or null if the
     *        {@link #DEFAULT_NAME_OF_DEFAULT_SCHEMA default name} should be used
     */
    public void setDefaultSchemaName( String defaultSchemaName ) {
        this.defaultSchemaName = defaultSchemaName == null ? DEFAULT_NAME_OF_DEFAULT_SCHEMA : defaultSchemaName;
    }

    /**
     * Get the class name of the metadata collector.
     * 
     * @return the name the class name of the metadata collector
     */
    public String getMetadataCollectorClassName() {
        return metadataCollectorClassName;
    }

    /**
     * Set the class name of the metadata collector and instantiates a new metadata collector object for that class
     * 
     * @param metadataCollectorClassName the class name for the metadata collector, or null if the
     *        {@link #DEFAULT_METADATA_COLLECTOR default metadata collector} should be used
     * @throws ClassNotFoundException if the the named metadata collector class cannot be located
     * @throws IllegalAccessException if the metadata collector class or its nullary constructor is not accessible.
     * @throws InstantiationException if the metadata collector class represents an abstract class, an interface, an array class,
     *         a primitive type, or void; or if the class has no nullary constructor; or if the instantiation fails for some other
     *         reason.
     * @throws ClassCastException if the given class cannot be cast to {@link MetadataCollector}.
     * @see Class#forName(String)
     * @see Class#newInstance()
     */
    @SuppressWarnings( "unchecked" )
    public synchronized void setMetadataCollectorClassName( String metadataCollectorClassName )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (metadataCollectorClassName == null) {
            this.metadataCollectorClassName = DEFAULT_METADATA_COLLECTOR.getClass().getName();
            this.metadataCollector = DEFAULT_METADATA_COLLECTOR;
        } else {
            Class newCollectorClass = Class.forName(metadataCollectorClassName);
            this.metadataCollector = (MetadataCollector)newCollectorClass.newInstance();
            this.metadataCollectorClassName = metadataCollectorClassName;
        }
    }

    /**
     * Returns the metadata collector instance
     * 
     * @return the metadata collector
     */
    public synchronized MetadataCollector getMetadataCollector() {
        return metadataCollector;
    }

    public boolean areUpdatesAllowed() {
        return false;
    }

    /**
     * In-memory connectors aren't shared and cannot be loaded from external sources if updates are not allowed. Therefore, in
     * order to avoid setting up an in-memory connector that is permanently empty (presumably, not a desired outcome), all
     * in-memory connectors must allow updates.
     * 
     * @param updatesAllowed must be true
     * @throws RepositorySourceException if {@code updatesAllowed != true}.
     */
    public void setUpdatesAllowed( boolean updatesAllowed ) {
        if (updatesAllowed == false) {
            throw new RepositorySourceException(JdbcMetadataI18n.sourceIsReadOnly.text(this.name));
        }

    }

}
