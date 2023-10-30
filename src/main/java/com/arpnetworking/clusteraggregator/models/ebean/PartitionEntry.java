/*
 * Copyright 2015 Groupon.com
 *
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
package com.arpnetworking.clusteraggregator.models.ebean;

import com.arpnetworking.utility.Database;
import io.ebean.annotation.WhenCreated;
import io.ebean.annotation.WhenModified;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.sql.Timestamp;
import javax.annotation.Nullable;

/**
 * Model that holds the data for a partition.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
// CHECKSTYLE.OFF: MemberNameCheck
@Entity
@Table(name = "partition_entry", schema = "clusteragg")
public class PartitionEntry {
    /**
     * Looks up a partition entry by key and partition set name.
     *
     * @param key the key
     * @param partitionSet the partition set
     * @param database the database backing the data
     * @return The partition entry if it exists, otherwise null
     */
    @Nullable
    public static PartitionEntry findByKey(final String key, final PartitionSet partitionSet, final Database database) {
        return database.getEbeanServer()
                .find(PartitionEntry.class)
                .where()
                .eq("key", key)
                .eq("partition.partitionSet", partitionSet)
                .findOne();
    }

    public Long getId() {
        return id;
    }

    public void setId(final Long value) {
        id = value;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final Timestamp value) {
        createdAt = value;
    }

    public Timestamp getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(final Timestamp value) {
        updatedAt = value;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(final Long value) {
        version = value;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(final Partition value) {
        partition = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String value) {
        key = value;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Version
    @Column(name = "version")
    private Long version;

    @WhenCreated
    @Column(name = "created_at")
    private Timestamp createdAt;

    @WhenModified
    @Column(name = "updated_at")
    private Timestamp updatedAt;

    @Column(name = "`key`")
    private String key;

    @ManyToOne
    @JoinColumn(name = "partition_id")
    private Partition partition;
}
// CHECKSTYLE.ON: MemberNameCheck
