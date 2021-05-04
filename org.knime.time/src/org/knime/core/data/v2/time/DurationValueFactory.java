package org.knime.core.data.v2.time;

import java.time.Duration;

import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.time.duration.DurationCell;
import org.knime.core.data.time.duration.DurationCellFactory;
import org.knime.core.data.time.duration.DurationValue;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.table.access.DurationAccess.DurationReadAccess;
import org.knime.core.table.access.DurationAccess.DurationWriteAccess;
import org.knime.core.table.schema.DurationDataSpec;

/**
 * {@link ValueFactory} implementation for {@link ListCell} with elements of type {@link DurationCell}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class DurationValueFactory
    implements ValueFactory<DurationReadAccess, DurationWriteAccess> {

    /** A stateless instance of {@link DurationValueFactory} */
    public static final DurationValueFactory INSTANCE = new DurationValueFactory();

    @Override
    public DurationReadValue createReadValue(final DurationReadAccess access) {
        return new DefaultDurationReadValue(access);
    }

    @Override
    public DurationWriteValue createWriteValue(final DurationWriteAccess access) {
        return new DefaultDurationWriteValue(access);
    }

    @Override
    public DurationDataSpec getSpec() {
        return DurationDataSpec.INSTANCE;
    }

    /**
     * {@link ReadValue} equivalent to {@link DurationCell}.
     *
     * @since 4.3
     */
    public static interface DurationReadValue extends ReadValue, DurationValue, BoundedValue, StringValue {
    }

    /**
     * {@link WriteValue} equivalent to {@link DurationCell}.
     *
     * @since 4.3
     */
    public static interface DurationWriteValue extends WriteValue<DurationValue> {

        /**
         * @param duration the duration to set
         */
        void setDuration(Duration duration);

    }

    private static final class DefaultDurationReadValue implements DurationReadValue {

        private final DurationReadAccess m_access;

        private DefaultDurationReadValue(final DurationReadAccess access) {
            m_access = access;
        }

        @Override
        public DataCell getDataCell() {
            return DurationCellFactory.create(m_access.getDurationValue());
        }

        @Override
        public Duration getDuration() {
            return m_access.getDurationValue();
        }

        @Override
        public String getStringValue() {
            return m_access.getDurationValue().toString();
        }

    }

    private static final class DefaultDurationWriteValue implements DurationWriteValue {

        private final DurationWriteAccess m_access;

        private DefaultDurationWriteValue(final DurationWriteAccess access) {
            m_access = access;
        }

        @Override
        public void setValue(final DurationValue value) {
            m_access.setDurationValue(value.getDuration());
        }

        @Override
        public void setDuration(final Duration duration) {
            m_access.setDurationValue(duration);
        }

    }
}
