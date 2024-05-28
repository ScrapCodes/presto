/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

public class IcebergTableProperties
{
    public static final String FILE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";
    public static final String LOCATION_PROPERTY = "location";
    public static final String FORMAT_VERSION = "format_version";
    public static final String COMMIT_RETRIES = "commit_retries";
    public static final String DELETE_MODE = "delete_mode";
    private static final String DEFAULT_FORMAT_VERSION = "2";

    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public IcebergTableProperties(IcebergConfig icebergConfig)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        FILE_FORMAT_PROPERTY,
                        "File format for the table",
                        createUnboundedVarcharType(),
                        FileFormat.class,
                        icebergConfig.getFileFormat(),
                        false,
                        value -> FileFormat.valueOf(((String) value).toUpperCase(ENGLISH)),
                        value -> value.toString()))
                .add(new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "Partition transforms",
                        new ArrayType(VARCHAR),
                        List.class,
                        Collections.emptyList(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(stringProperty(
                        FORMAT_VERSION,
                        "Format version for the table",
                        DEFAULT_FORMAT_VERSION,
                        false))
                .add(integerProperty(
                        COMMIT_RETRIES,
                        "Determines the number of attempts in case of concurrent upserts and deletes",
                        TableProperties.COMMIT_NUM_RETRIES_DEFAULT,
                        false))
                .add(new PropertyMetadata<>(
                        DELETE_MODE,
                        "Delete mode for the table",
                        createUnboundedVarcharType(),
                        RowLevelOperationMode.class,
                        RowLevelOperationMode.MERGE_ON_READ,
                        false,
                        value -> RowLevelOperationMode.fromName((String) value),
                        RowLevelOperationMode::modeName))
                .build();

        columnProperties = ImmutableList.of(stringProperty(
                PARTITIONING_PROPERTY,
                "This column's partition transform",
                null,
                false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    public static FileFormat getFileFormat(Map<String, Object> tableProperties)
    {
        return (FileFormat) tableProperties.get(FILE_FORMAT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitioning(Map<String, Object> tableProperties)
    {
        List<String> partitioning = (List<String>) tableProperties.get(PARTITIONING_PROPERTY);
        return partitioning == null ? Collections.emptyList() : ImmutableList.copyOf(partitioning);
    }

    public static String getTableLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(LOCATION_PROPERTY);
    }

    public static String getFormatVersion(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(FORMAT_VERSION);
    }

    public static Integer getCommitRetries(Map<String, Object> tableProperties)
    {
        return (Integer) tableProperties.get(COMMIT_RETRIES);
    }

    public static RowLevelOperationMode getDeleteMode(Map<String, Object> tableProperties)
    {
        return (RowLevelOperationMode) tableProperties.get(DELETE_MODE);
    }
}
