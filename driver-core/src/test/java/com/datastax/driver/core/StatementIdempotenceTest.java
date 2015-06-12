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
package com.datastax.driver.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.querybuilder.BuiltStatement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class StatementIdempotenceTest {
    @Test(groups = "unit")
    public void should_default_to_false_when_not_set_on_statement_nor_query_options() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        assertThat(statement.isIdempotentWithDefault(queryOptions)).isFalse();
    }

    @Test(groups = "unit")
    public void should_use_query_options_when_not_set_on_statement() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        for (boolean valueInOptions : new boolean[]{ true, false }) {
            queryOptions.setDefaultIdempotence(valueInOptions);
            assertThat(statement.isIdempotentWithDefault(queryOptions)).isEqualTo(valueInOptions);
        }
    }

    @Test(groups = "unit")
    public void should_use_statement_when_set_on_statement() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        for (boolean valueInOptions : new boolean[]{ true, false })
            for (boolean valueInStatement : new boolean[]{ true, false }) {
                queryOptions.setDefaultIdempotence(valueInOptions);
                statement.setIdempotent(valueInStatement);
                assertThat(statement.isIdempotentWithDefault(queryOptions)).isEqualTo(valueInStatement);
            }
    }

    @Test(groups = "unit")
    public void should_infer_for_built_statement() {
        for (BuiltStatement statement : idempotentBuiltStatements())
            assertThat(statement.isIdempotent())
                .as(statement.getQueryString())
                .isTrue();

        for (BuiltStatement statement : nonIdempotentBuiltStatements())
            assertThat(statement.isIdempotent())
                .as(statement.getQueryString())
                .isFalse();
    }

    @Test(groups = "unit")
    public void should_override_inferred_value_when_manually_set_on_built_statement() {
        for (boolean manualValue : new boolean[]{ true, false }) {
            for (BuiltStatement statement : idempotentBuiltStatements()) {
                statement.setIdempotent(manualValue);
                assertThat(statement.isIdempotent()).isEqualTo(manualValue);
            }

            for (BuiltStatement statement : nonIdempotentBuiltStatements()) {
                statement.setIdempotent(manualValue);
                assertThat(statement.isIdempotent()).isEqualTo(manualValue);
            }
        }
    }

    private static ImmutableList<BuiltStatement> idempotentBuiltStatements() {
        return ImmutableList.of(
            update("foo").with(set("v", 1)).where(eq("k", 1)), // set simple value
            update("foo").with(add("s", 1)).where(eq("k", 1)), // add to set
            update("foo").with(put("m", "a", 1)).where(eq("k", 1)), // put in map

            // idempotent function calls

            insertInto("foo").value("k", 1).value("v", fcall("token", "k")),
            insertInto("foo").value("k", 1).value("v", fcall(true, "foo")),
            insertInto("foo").value("k", 1).value("v", raw("foo()")),
            update("foo").with(set("v", fcall("token", "k"))).where(eq("k", 1)),
            update("foo").with(set("v", fcall(true, "foo"))).where(eq("k", 1)),
            update("foo").with(set("v", raw("foo()"))).where(eq("k", 1))

        );
    }

    private static ImmutableList<BuiltStatement> nonIdempotentBuiltStatements() {
        return ImmutableList.of(
            update("foo").with(append("l", 1)).where(eq("k", 1)), // append to list
            update("foo").with(set("v", 1)).and(prepend("l", 1)).where(eq("k", 1)), // prepend to list
            update("foo").with(incr("c")).where(eq("k", 1)), // counter update

            // non-idempotent function calls

            update("foo").with(set("v", fcall("NOW"))).where(eq("k", 1)),
            update("foo").with(set("v", fcall("UUID"))).where(eq("k", 2)),
            update("foo").with(set("v", fcall(false, "foo"))).where(eq("k", 3)),
            update("foo").with(set("v", raw("NOW()"))).where(eq("k", 4)),
            update("foo").with(set("v", raw("UUID()"))).where(eq("k", 5)),
            update("foo").with(set("v", now())).where(eq("k", 6)),
            update("foo").with(set("v", uuid())).where(eq("k", 7)),
            update("foo").with(set("v", fcall("foo", now()))).where(eq("k", 8)), // nested now()
            update("foo").with(set("v", Lists.newArrayList(fcall("foo", now())))).where(eq("k", 8)), // list with nested now()

            insertInto("foo").value("k", 9).value("v", fcall("NOW")),
            insertInto("foo").value("k", 10).value("v", fcall("UUID")),
            insertInto("foo").value("k", 11).value("v", fcall(false, "foo")),
            insertInto("foo").value("k", 12).value("v", raw("NOW()")),
            insertInto("foo").value("k", 13).value("v", raw("UUID()")),
            insertInto("foo").value("k", 14).value("v", now()),
            insertInto("foo").value("k", 15).value("v", uuid()),
            insertInto("foo").value("k", 16).value("v", fcall("foo", uuid())), // nested uuid()
            insertInto("foo").value("v", Sets.newHashSet(fcall("foo", uuid()))), // set with nest uuid()


            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{17, fcall("NOW")}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{18, fcall("UUID")}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{19, fcall(false, "foo")}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{20, raw("NOW()")}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{21, raw("UUID()")}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{22, now()}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{23, uuid()}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{24, fcall("foo", uuid())}),
            insertInto("foo").values(new String[]{"k", "v"}, new Object[]{24, ImmutableMap.of("foo", fcall("foo", uuid()))}) // map with nested uuid()

        );
    }
}
