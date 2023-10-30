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
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.sql.Timestamp;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Model that holds the count of entries in a partition.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
// CHECKSTYLE.OFF: MemberNameCheck
@Entity
@Table(name = "partition", schema = "clusteragg")
public class Partition {
    /**
     * Fetches a partition by partitionSet and partitionNumber.
     *
     * @param partitionSet the partition set
     * @param partitionNumber the partition number
     * @param database the database backing the data
     * @return the partition if found, otherwise null
     */
    @Nullable
    public static Partition getPartition(final PartitionSet partitionSet, final int partitionNumber, final Database database) {
        return database.getEbeanServer()
                .find(Partition.class)
                .where()
                .eq("partitionNumber", partitionNumber)
                .eq("partitionSet", partitionSet)
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

    public Integer getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(final Integer value) {
        partitionNumber = value;
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

    public List<PartitionEntry> getEntries() {
        return entries;
    }

    public void setEntries(final List<PartitionEntry> value) {
        entries = value;
    }

    public PartitionSet getPartitionSet() {
        return partitionSet;
    }

    public void setPartitionSet(final PartitionSet value) {
        partitionSet = value;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(final Integer value) {
        count = value;
    }

    /**
     * Locks the record with a select for update.
     *
     * @param database the database backing the data
     */
    public void lock(final Database database) {
        if (database.getEbeanServer().currentTransaction() == null) {
            throw new IllegalStateException("Must be in a transaction before locking");
        }
        database.getEbeanServer().find(Partition.class).forUpdate().where().eq("id", id).findOne();
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

    @Column(name = "partition_number")
    private Integer partitionNumber;

    @Column(name = "count")
    private Integer count;

    @OneToMany(mappedBy = "partition")
    private List<PartitionEntry> entries;

    @ManyToOne
    @JoinColumn(name = "partition_set_id")
    private PartitionSet partitionSet;
}
// CHECKSTYLE.ON: MemberNameCheck
