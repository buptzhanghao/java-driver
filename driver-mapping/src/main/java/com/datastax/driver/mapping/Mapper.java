/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * An object handling the mapping of a particular class.
 * <p/>
 * A {@code Mapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#mapper} method.
 */
public class Mapper<T> {

    private static final Logger logger = LoggerFactory.getLogger(EntityMapper.class);

    final MappingManager manager;
    final ProtocolVersion protocolVersion;
    final Class<T> klass;
    final EntityMapper<T> mapper;
    final TableMetadata tableMetadata;

    // Cache prepared statements for each type of query we use.
    private volatile Map<MapperQueryKey, PreparedStatement> preparedQueries = Collections.<MapperQueryKey, PreparedStatement>emptyMap();

    private static final Function<Object, Void> NOOP = Functions.<Void>constant(null);

    private volatile Set<Option> defaultSaveOptions;
    private volatile Set<Option> defaultGetOptions;
    private volatile Set<Option> defaultDeleteOptions;

    final Function<ResultSet, T> mapOneFunction;
    final Function<ResultSet, Result<T>> mapAllFunction;

    Mapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
        this.manager = manager;
        this.klass = klass;
        this.mapper = mapper;

        KeyspaceMetadata keyspace = session().getCluster().getMetadata().getKeyspace(mapper.getKeyspace());
        this.tableMetadata = keyspace == null ? null : keyspace.getTable(Metadata.quote(mapper.getTable()));

        this.protocolVersion = manager.getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
        this.mapOneFunction = new Function<ResultSet, T>() {
            public T apply(ResultSet rs) {
                return Mapper.this.map(rs).one();
            }
        };
        this.mapAllFunction = new Function<ResultSet, Result<T>>() {
            public Result<T> apply(ResultSet rs) {
                return Mapper.this.map(rs);
            }
        };

        this.defaultSaveOptions = Collections.<Option>emptySet();
        this.defaultGetOptions = Collections.<Option>emptySet();
        this.defaultDeleteOptions = Collections.<Option>emptySet();
    }

    Session session() {
        return manager.getSession();
    }

    PreparedStatement getPreparedQuery(QueryType type, Set<Option> options) {

        MapperQueryKey pqk = new MapperQueryKey(type, options);

        PreparedStatement stmt = preparedQueries.get(pqk);
        if (stmt == null) {
            synchronized (preparedQueries) {
                stmt = preparedQueries.get(pqk);
                if (stmt == null) {
                    String queryString = type.makePreparedQueryString(tableMetadata, mapper, options);
                    logger.debug("Preparing query {}", queryString);
                    stmt = session().prepare(queryString);
                    Map<MapperQueryKey, PreparedStatement> newQueries = new HashMap<MapperQueryKey, PreparedStatement>(preparedQueries);
                    newQueries.put(pqk, stmt);
                    preparedQueries = newQueries;
                }
            }
        }
        return stmt;
    }

    /**
     * The {@code TableMetadata} for this mapper.
     *
     * @return the {@code TableMetadata} for this mapper or {@code null} if keyspace is not set.
     */
    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    /**
     * The {@code MappingManager} managing this mapper.
     *
     * @return the {@code MappingManager} managing this mapper.
     */
    public MappingManager getManager() {
        return manager;
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity) {
        return saveQuery(entity, this.defaultSaveOptions);
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity, Option... options) {
        return saveQuery(entity, ImmutableSet.<Option>copyOf(options));
    }

    private Statement saveQuery(T entity, Set<Option> options) {
        BoundStatement bs = getPreparedQuery(QueryType.SAVE, options).bind();
        int i = 0;
        for (ColumnMapper<T> cm : mapper.allColumns()) {
            Object value = cm.getValue(entity);
            bs.setBytesUnsafe(i++, value == null ? null : cm.getDataType().serialize(value, protocolVersion));
        }

        for (Option opt : options) {
            if (opt.isValidFor(QueryType.SAVE))
                opt.addToPreparedStatement(bs, i++);
        }

        if (mapper.writeConsistency != null)
            bs.setConsistencyLevel(mapper.writeConsistency);
        return bs;
    }

    /**
     * Save an entity mapped by this mapper.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     */
    public void save(T entity) {
        session().execute(saveQuery(entity));
    }

    /**
     * Save an entity mapped by this mapper and using special options for save.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     * <p/>
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when saving.
     */
    public void save(T entity, Option... options) {
        session().execute(saveQuery(entity, ImmutableSet.copyOf(options)));
    }

    /**
     * Save an entity mapped by this mapper asynchonously.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity) {
        return Futures.transform(session().executeAsync(saveQuery(entity)), NOOP);
    }

    /**
     * Save an entity mapped by this mapper asynchonously using special options for save.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity, options))}.
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when saving.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity, Option... options) {
        return Futures.transform(session().executeAsync(saveQuery(entity, ImmutableSet.copyOf(options))), NOOP);
    }

    /**
     * Creates a query to fetch entity given its PRIMARY KEY.
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually,
     * but in other cases, calling {@link #get} or {@link #getAsync} is shorter.
     * <p/>
     * This method allows you to provide a suite of {@link Option} to include in
     * the GET query. Options currently supported for GET are :
     * <ul>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return a query that fetch the entity of PRIMARY KEY {@code objects}.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public Statement getQuery(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        Set<Option> options = new HashSet<Option>();
        for (Object o : objects) {
            if (o instanceof Option) {
                options.add((Option)o);
            } else {
                pks.add(o);
            }
        }
        return getQuery(pks, options);
    }

    private Statement getQuery(List<Object> primaryKeys, Set<Option> options) {

        if (primaryKeys.size() != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKeys.size()));

        // Options are not useful to prepare a GET query for now, with the existing options.
        // So they will be ignored in #getPreparedQuery() but I add them to the call in case
        // we need to add new options that would be included in the prepared query.
        // So to code a new Option, we only need to write the class and write isValidFor() consequently.
        Set<Option> toUseOptions;
        if (options.size() != 0) {
            toUseOptions = options;
        } else {
            toUseOptions = this.defaultGetOptions;
        }

        // Since currently no options are to be included in GET queries,
        // this will always be empty for now.
        BoundStatement bs = getPreparedQuery(QueryType.GET, toUseOptions).bind();
        int i = 0;
        for (Object value : primaryKeys) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            }
            bs.setBytesUnsafe(i, column.getDataType().serialize(value, protocolVersion));
            i++;
        }

        for (Option opt : toUseOptions) {
            if (opt.isValidFor(QueryType.GET))
                opt.addToPreparedStatement(bs, i++);
        }

        if (mapper.readConsistency != null)
            bs.setConsistencyLevel(mapper.readConsistency);
        return bs;
    }

    /**
     * Fetch an entity based on its primary key.
     * <p/>
     * This method is basically equivalent to: {@code map(getManager().getSession().execute(getQuery(objects))).one()}.
     * <p/>
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return the entity fetched or {@code null} if it doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public T get(Object... objects) {
        return map(session().execute(getQuery(objects))).one();
    }

    /**
     * Fetch an entity based on its primary key asynchronously.
     * <p/>
     * This method is basically equivalent to mapping the result of: {@code getManager().getSession().executeAsync(getQuery(objects))}.
     * <p/>
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return a future on the fetched entity. The return future will yield
     * {@code null} if said entity doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public ListenableFuture<T> getAsync(Object... objects) {
        return Futures.transform(session().executeAsync(getQuery(objects)), mapOneFunction);
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p/>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Note : currently, only {@link com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     * <p/>
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a query that delete {@code entity} (based on it's defined mapping) with
     * provided USING options.
     */
    public Statement deleteQuery(T entity, Option... options) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }

        return deleteQuery(pks, ImmutableSet.copyOf(options));
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p/>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     *
     * @param entity the entity to delete.
     * @return a query that delete {@code entity} (based on it's defined mapping).
     */
    public Statement deleteQuery(T entity) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }

        return deleteQuery(pks, Collections.<Option>emptySet());
    }

    /**
     * Creates a query that can be used to delete an entity given its PRIMARY KEY.
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key). The values can also contain, after
     * specifying the primary keys columns, a suite of {@link Option} to include in
     * the DELETE query. Note : currently, only {@link com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return a query that delete the entity of PRIMARY KEY {@code primaryKey}.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public Statement deleteQuery(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        Set<Option> options = new HashSet<Option>();
        for (Object o : objects) {
            if (o instanceof Option) {
                options.add((Option)o);
            } else {
                pks.add(o);
            }
        }
        return deleteQuery(pks, options);
    }

    private Statement deleteQuery(List<Object> primaryKey, Set<Option> options) {
        if (primaryKey.size() != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.size()));

        Set<Option> toUseOptions;
        if (options.size() != 0) {
            toUseOptions = options;
        } else {
            toUseOptions = this.defaultDeleteOptions;
        }

        BoundStatement bs = getPreparedQuery(QueryType.DEL, toUseOptions).bind();
        int i = 0;
        for (Option opt : toUseOptions) {
            if (opt.isValidFor(QueryType.DEL))
                opt.addToPreparedStatement(bs, i);
            if (opt.isIncludedInQuery())
                i++;
        }

        int columnNumber = 0;
        for (Object value : primaryKey) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(columnNumber);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            }
            bs.setBytesUnsafe(i++, column.getDataType().serialize(value, protocolVersion));
            columnNumber++;
        }

        if (mapper.writeConsistency != null)
            bs.setConsistencyLevel(mapper.writeConsistency);
        return bs;
    }

    /**
     * Deletes an entity mapped by this mapper.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     */
    public void delete(T entity) {
        session().execute(deleteQuery(entity));
    }

    /**
     * Deletes an entity mapped by this mapper using provided options.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     */
    public void delete(T entity, Option... options) {
        session().execute(deleteQuery(entity, options));
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity) {
        return Futures.transform(session().executeAsync(deleteQuery(entity)), NOOP);
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously using provided options.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity, Option... options) {
        return Futures.transform(session().executeAsync(deleteQuery(entity, options)), NOOP);
    }

    /**
     * Deletes an entity based on its primary key.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order
     *                of the primary key.Can be followed by {@link Option} to include
     *                in the DELETE query.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public void delete(Object... objects) {
        session().execute(deleteQuery(objects));
    }

    /**
     * Deletes an entity based on its primary key asynchronously.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order
     *                of the primary key. Can be followed by {@link Option} to include
     *                in the DELETE query.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public ListenableFuture<Void> deleteAsync(Object... objects) {
        return Futures.transform(session().executeAsync(deleteQuery(objects)), NOOP);
    }

    /**
     * Map the rows from a {@code ResultSet} into the class this is mapper of.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     */
    public Result<T> map(ResultSet resultSet) {
        return new Result<T>(resultSet, mapper, protocolVersion);
    }

    /**
     * Set the default save {@link Option} for this object mapper, that will be used
     * in all save operations. Refer to {@link Mapper#save)} with Option argument
     * to check available save options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultSaveOptions}.
     */
    public void setDefaultSaveOptions(Option... options) {
        this.defaultSaveOptions = ImmutableSet.<Option>copyOf(options);
    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultSaveOptions() {
        this.defaultSaveOptions = Collections.<Option>emptySet();
    }

    /**
     * Set the default get {@link Option} for this object mapper, that will be used
     * in all get operations. Refer to {@link Mapper#get)} with Option argument
     * to check available get options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultGetOptions}.
     */
    public void setDefaultGetOptions(Option... options) {
        this.defaultGetOptions = ImmutableSet.<Option>copyOf(options);

    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultGetOptions() {
        this.defaultGetOptions = Collections.<Option>emptySet();
    }

    /**
     * Set the default delete {@link Option} for this object mapper, that will be used
     * in all delete operations. Refer to {@link Mapper#delete)} with Option argument
     * to check available delete options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultDeleteOptions}
     *                instead of putting null argument here.
     */
    public void setDefaultDeleteOptions(Option... options) {
        this.defaultDeleteOptions = ImmutableSet.<Option>copyOf(options);
    }

    /**
     * Reset the default delete options for this object mapper.
     */
    public void resetDefaultDeleteOptions() {
        this.defaultDeleteOptions = Collections.<Option>emptySet();
    }

    /**
     * An object to allow defining specific options during a
     * {@link Mapper#save(Object)} or {@link Mapper#delete(Object...)} operation.
     * <p/>
     * The options will be added as : 'INSERT [...] USING option-name option-value [AND option-name option value... ].
     */
    public static abstract class Option {

        enum Type {TTL, TIMESTAMP, CL, TRACING}

        Type type;

        /**
         * Creates a new Option object to add a TTL value in a mapper operation.
         *
         * @param value the value to use for the operation.
         * @return the Option object configured to set a TTL value to a
         * mapper operation.
         */
        public static Option ttl(int value) {
            return new Ttl(value);
        }

        /**
         * Creates a new Option object to add a TIMESTAMP value in a mapper operation.
         *
         * @param value the value to use for the operation.
         * @return the Option object configured to set a TIMESTAMP value to a
         * mapper operation.
         */
        public static Option timestamp(long value) {
            return new Timestamp(value);
        }

        /**
         * Creates a new Option object to add a Consistency level value in a mapper operation.
         *
         * @param value the {@link com.datastax.driver.core.ConsistencyLevel} to use for the operation.
         * @return the Option object configured to set a Consistency level value to a
         * mapper operation.
         */
        public static Option consistencyLevel(ConsistencyLevel value) {
            return new ConsistencyLevelOption(value);
        }

        /**
         * Creates a new Option object to set Tracing flag in a mapper operation.
         *
         * @param value true to set tracing on, false otherwise.
         * @return the Option object configured to add the tracing flag to a
         * mapper operation.
         */
        public static Option tracing(boolean value) {
            return new Tracing(value);
        }

        abstract void appendTo(Insert.Options usings);

        abstract void appendTo(Delete.Options usings);

        abstract void addToPreparedStatement(BoundStatement bs, int i);

        abstract boolean isValidFor(QueryType qt);

        abstract boolean isIncludedInQuery();

        static class Ttl extends Option {

            private int ttlValue;

            Ttl(int value) {
                this.type = Type.TTL;
                this.ttlValue = value;
            }

            /**
             * Get the TTL value configured in the object.
             *
             * @return the TTL value.
             */
            public int getValue() {
                return this.ttlValue;
            }

            void appendTo(Insert.Options usings) {
                usings.and(QueryBuilder.ttl(QueryBuilder.bindMarker()));
            }

            void appendTo(Delete.Options usings) {
                usings.and(QueryBuilder.ttl(QueryBuilder.bindMarker()));
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setInt(i, this.ttlValue);
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE;
            }

            boolean isIncludedInQuery() {
                return true;
            }

        }

        static class Timestamp extends Option {

            private long tsValue;

            Timestamp(long value) {
                this.type = Type.TIMESTAMP;
                this.tsValue = value;
            }

            /**
             * Get the TIMESTAMP value configured in the object.
             *
             * @return the TIMESTAMP value.
             */
            public long getValue() {
                return this.tsValue;
            }

            void appendTo(Insert.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            void appendTo(Delete.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE || qt == QueryType.DEL;
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setLong(i, this.tsValue);
            }

            boolean isIncludedInQuery() {
                return true;
            }

        }

        static class ConsistencyLevelOption extends Option {

            private ConsistencyLevel cl;

            ConsistencyLevelOption(ConsistencyLevel cl) {
                this.type = Type.CL;
                this.cl = cl;
            }

            /**
             * Get the Consistency level value configured in the object.
             *
             * @return the Consistency level value.
             */
            public ConsistencyLevel getValue() {
                return this.cl;
            }

            // Shouldn't be called
            void appendTo(Insert.Options usings) {
            }

            // Shouldn't be called
            void appendTo(Delete.Options usings) {
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setConsistencyLevel(cl);
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE
                    || qt == QueryType.DEL
                    || qt == QueryType.GET;
            }

            boolean isIncludedInQuery() {
                return false;
            }

        }

        static class Tracing extends Option {

            private boolean tracing;

            Tracing(boolean tracing) {
                this.type = Type.TRACING;
                this.tracing = tracing;
            }

            /**
             * Get the tracing value configured in the object.
             *
             * @return the tracing value.
             */
            public boolean getValue() {
                return this.tracing;
            }

            // Shouldn't be called
            void appendTo(Insert.Options usings) {
            }

            // Shouldn't be called
            void appendTo(Delete.Options usings) {
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                if (this.tracing)
                    bs.enableTracing();
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE
                    || qt == QueryType.DEL
                    || qt == QueryType.GET;
            }

            boolean isIncludedInQuery() {
                return false;
            }

        }

    }

    private class MapperQueryKey {
        private int key;

        MapperQueryKey(QueryType qt, Set<Option> options) {
            this.key = qt.hashCode();
            Set<Option.Type> optionsSet = new HashSet<Option.Type>();
            for (Option opt : options) {
                if (opt.isIncludedInQuery())
                    optionsSet.add(opt.type);
            }
            this.key += optionsSet.hashCode();
        }

        private int getKey() {
            return this.key;
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (obj.getClass() != this.getClass())) {
                return false;
            }
            MapperQueryKey keyObj = (MapperQueryKey)obj;
            return keyObj.getKey() == getKey();

        }

        public int hashCode() {
            return this.key;
        }
    }

}
