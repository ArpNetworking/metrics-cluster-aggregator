/*
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Represents a sample.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class Quantity implements Comparable<Quantity>, Serializable {

    public double getValue() {
        return _value;
    }

    public Optional<Unit> getUnit() {
        return Optional.ofNullable(_unit);
    }

    /**
     * Add this {@link Quantity} to the specified one returning the
     * result. Both {@link Quantity} instances must either not have a
     * {@link Unit} or the {@link Unit} must be of the same type.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting sum {@link Quantity}.
     */
    public Quantity add(final Quantity otherQuantity) {
        if ((_unit == null) != (otherQuantity._unit == null)) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; thisQuantity=%s otherQuantity=%s",
                    this,
                    otherQuantity));
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value + otherQuantity._value, Optional.ofNullable(_unit));
        }
        final Unit smallerUnit = _unit.getSmallerUnit(otherQuantity.getUnit().get());
        return new Quantity(
                smallerUnit.convert(_value, _unit)
                        + smallerUnit.convert(otherQuantity._value, otherQuantity._unit),
                Optional.of(smallerUnit));
    }

    /**
     * Subtract the specified {@link Quantity} from this one returning
     * the result. Both {@link Quantity} instances must either not have
     * a {@link Unit} or the {@link Unit} must be of the same type.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting difference {@link Quantity}.
     */
    public Quantity subtract(final Quantity otherQuantity) {
        if ((_unit == null) != (otherQuantity._unit == null)) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; thisQuantity=%s otherQuantity=%s",
                    this,
                    otherQuantity));
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value - otherQuantity._value, Optional.ofNullable(_unit));
        }
        final Unit smallerUnit = _unit.getSmallerUnit(otherQuantity.getUnit().get());
        return new Quantity(
                smallerUnit.convert(_value, _unit)
                        - smallerUnit.convert(otherQuantity._value, otherQuantity._unit),
                Optional.of(smallerUnit));
    }

    /**
     * Multiply this {@link Quantity} with the specified one returning
     * the result.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting product {@link Quantity}.
     */
    public Quantity multiply(final Quantity otherQuantity) {
        // TODO(vkoskela): Support division by quantity with unit [2F].
        if (otherQuantity._unit != null) {
            throw new UnsupportedOperationException("Compound units not supported yet");
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value * otherQuantity._value, Optional.empty());
        }
        return new Quantity(
                _value * otherQuantity._value,
                Optional.ofNullable(_unit));
    }

    /**
     * Divide this {@link Quantity} by the specified one returning
     * the result.
     *
     * @param otherQuantity The other {@link Quantity}.
     * @return The resulting quotient {@link Quantity}.
     */
    public Quantity divide(final Quantity otherQuantity) {
        // TODO(vkoskela): Support division by quantity with unit [2F].
        if (otherQuantity._unit != null) {
            throw new UnsupportedOperationException("Compound units not supported yet");
        }
        if (Objects.equals(_unit, otherQuantity._unit)) {
            return new Quantity(_value / otherQuantity._value, Optional.empty());
        }
        return new Quantity(
                _value / otherQuantity._value,
                Optional.ofNullable(_unit));
    }

    /**
     * Convert this {@link Quantity} to one in the specified unit. This
     * {@link Quantity} must also have a {@link Unit} and it must
     * be in the same domain as the provided unit.
     *
     * @param unit {@link Unit} to convert to.
     * @return {@link Quantity} in specified unit.
     */
    public Quantity convertTo(final Unit unit) {
        if (_unit == null) {
            throw new IllegalStateException(String.format(
                    "Cannot convert a quantity without a unit; this=%s",
                    this));
        }
        if (_unit.equals(unit)) {
            return this;
        }
        return new Quantity(
                unit.convert(_value, _unit),
                Optional.of(unit));
    }

    /**
     * Convert this {@link Quantity} to one in the specified optional unit.
     * Either this {@link Quantity} also has a {@link Unit} in the
     * same domain as the provided unit or both units are absent.
     *
     * @param unit {@link Optional} {@link Unit} to convert to.
     * @return {@link Quantity} in specified unit.
     */
    public Quantity convertTo(final Optional<Unit> unit) {
        if ((_unit == null) != unit.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "Units must both be present or absent; quantity=%s unit=%s",
                    this,
                    unit));
        }
        if (Objects.equals(_unit, unit.orElse(null))) {
            return this;
        }
        return new Quantity(
                unit.get().convert(_value, _unit),
                unit);
    }

    @Override
    public int compareTo(final Quantity other) {
        if (Objects.equals(_unit, other._unit)) {
            return Double.compare(_value, other._value);
        } else if (other._unit != null && _unit != null) {
            final Unit smallerUnit = _unit.getSmallerUnit(other._unit);
            final double convertedValue = smallerUnit.convert(_value, _unit);
            final double otherConvertedValue = smallerUnit.convert(other._value, other._unit);
            return Double.compare(convertedValue, otherConvertedValue);
        }
        throw new IllegalArgumentException(String.format(
                "Cannot compare a quantity with a unit to a quantity without a unit; this=%s, other=%s",
                this,
                other));
    }

    @Override
    public int hashCode() {
        return Objects.hash(_value, _unit);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Quantity)) {
            return false;
        }

        final Quantity sample = (Quantity) o;

        return Double.compare(sample._value, _value) == 0
                && Objects.equals(_unit, sample._unit);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("Unit", _unit)
                .add("Value", _value)
                .toString();
    }

    /**
     * Ensures all {@link Quantity} instances have the same (including no)
     * {@link Unit}.
     *
     * @param quantities {@link Quantity} instances to convert.
     * @return {@link List} of {@link Quantity} instances with the
     * same {@link Unit} (or no {@link Unit}).
     */
    public static List<Quantity> unify(final Collection<Quantity> quantities) {
        // This is a 2-pass operation:
        // First pass is to grab the smallest unit in the samples
        // Second pass is to convert everything to that unit
        Optional<Unit> smallestUnit = Optional.empty();
        for (final Quantity quantity : quantities) {
            if (!smallestUnit.isPresent()) {
                smallestUnit = quantity.getUnit();
            } else if (quantity.getUnit().isPresent()
                    && !smallestUnit.get().getSmallerUnit(quantity.getUnit().get()).equals(smallestUnit.get())) {
                smallestUnit = quantity.getUnit();
            }
        }

        final List<Quantity> convertedSamples = Lists.newArrayListWithExpectedSize(quantities.size());
        final Function<Quantity, Quantity> converter = SampleConverter.to(smallestUnit);
        for (final Quantity quantity : quantities) {
            convertedSamples.add(converter.apply(quantity));
        }

        return convertedSamples;
    }

    private Quantity(final Builder builder) {
        this(builder._value, Optional.ofNullable(builder._unit));
    }

    private Quantity(final double value, final Optional<Unit> unit) {
        _value = value;
        _unit = unit.orElse(null);
    }

    @Nullable
    private final Unit _unit;
    private final double _value;

    private static final long serialVersionUID = -6339526234042605516L;

    private static final class SampleConverter implements Function<Quantity, Quantity> {

        public static Function<Quantity, Quantity> to(final Optional<Unit> unit) {
            if (unit.isPresent()) {
                return new SampleConverter(unit.get());
            }
            return Functions.identity();
        }

        @Override
        @Nullable
        public Quantity apply(@Nullable final Quantity quantity) {
            if (quantity == null) {
                return null;
            }
            if (!quantity.getUnit().isPresent()) {
                throw new IllegalArgumentException(String.format("Cannot convert a quantity without unit; sample=%s", quantity));
            }
            return new Quantity.Builder()
                    .setValue(_unit.convert(quantity.getValue(), quantity.getUnit().get()))
                    .setUnit(_unit)
                    .build();
        }

        private SampleConverter(final Unit convertTo) {
            _unit = convertTo;
        }

        private final Unit _unit;
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link Quantity}.
     */
    public static final class Builder extends OvalBuilder<Quantity> {

        /**
         * Public constructor.
         */
        public Builder() {
            super((java.util.function.Function<Builder, Quantity>) Quantity::new);
        }

        /**
         * Set the value. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder setValue(final Double value) {
            _value = value;
            return this;
        }

        /**
         * Set the unit. Optional. Default is no unit.
         *
         * @param value The unit.
         * @return This {@link Builder} instance.
         */
        public Builder setUnit(@Nullable final Unit value) {
            _unit = value;
            return this;
        }

        @NotNull
        private Double _value;
        @Nullable
        private Unit _unit;
    }
}
