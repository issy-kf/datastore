package com.fujitsu.hope.datastore.meta;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;

import org.junit.Test;

public class AttributeMetaTest {
	@Test
	public void testToColumnType() {
		assertThat(AttributeMeta.toColumnType(Integer.class), is(ColumnType.INTEGER));
		assertThat(AttributeMeta.toColumnType(String.class), is(ColumnType.STRING));
		assertThat(AttributeMeta.toColumnType(Long.class), is(ColumnType.LONG));
		assertThat(AttributeMeta.toColumnType(Short.class), is(ColumnType.SHORT));
		assertThat(AttributeMeta.toColumnType(Byte.class), is(ColumnType.BYTE));
		assertThat(AttributeMeta.toColumnType(Boolean.class), is(ColumnType.BOOLEAN));
		assertThat(AttributeMeta.toColumnType(BigDecimal.class), is(ColumnType.BIG_DECIMAL));
	}
}
