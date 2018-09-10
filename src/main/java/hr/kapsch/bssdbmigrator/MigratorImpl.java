package hr.kapsch.bssdbmigrator;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.metadata.TableMetaDataContext;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Service;

@Service
public class MigratorImpl implements Migrator {
	private final Logger logger = LoggerFactory.getLogger(MigratorImpl.class);

	private final JdbcTemplate importJdbcTemplate;
	private final JdbcTemplate exportJdbcTemplate;

	public MigratorImpl(
			@Qualifier("importJdbcTemplate") JdbcTemplate importJdbcTemplate,
			@Qualifier("exportJdbcTemplate") JdbcTemplate exportJdbcTemplate) {
		this.importJdbcTemplate = importJdbcTemplate;
		this.exportJdbcTemplate = exportJdbcTemplate;
	}


	@Override
	public void migrate() {
//		migrateTable("partners", Collections.singletonMap("cdr_traffic_enabled", true), Collections.emptyMap());
//		migrateTable("users", Collections.emptyMap(), Collections.emptyMap());
//		migrateTable("global_settings", Collections.emptyMap(), Collections.singletonMap("MONETARY_AMOUNT", "max_bt_unit_price"));
//		migrateTable("financial_reports", Collections.emptyMap(), Collections.emptyMap());

		migrateSequence("seq_partner");

//		List<String> importNames = importJdbcTemplate.queryForList("select name from partners", String.class);
//		System.out.println("importNames = " + importNames);
//
//		List<String> exportNames = exportJdbcTemplate.queryForList("select name from partners", String.class);
//		System.out.println("exportNames = " + exportNames);

	}

	private void migrateSequence(String seqName) {
		logger.info("Migrating sequence {}", seqName);
		Number currentValue = this.importJdbcTemplate.queryForObject("select " + seqName + ".nextval from dual", Number.class);
		String exportSql = "select setval('" + seqName + "', " + currentValue.intValue() + ", true)";
		this.exportJdbcTemplate.execute(exportSql);
	}


	private void migrateTable(String table, Map<String, Object> newRequiredColumns, Map<String, String> renamedColumns) {
		logger.info("Migrating table {}", table);

		final List<String> importColumnNames = extractColumnNames(this.importJdbcTemplate, table);

		final List<String> exportColumnNames = resolveExportColumnNames(newRequiredColumns, renamedColumns, importColumnNames);

		List<Map<String, Object>> exportValues = new ArrayList<>();
		RowCallbackHandler rch = rs -> {
			Map<String, Object> row = convertToExportRowMap(importColumnNames, renamedColumns, rs);
			row.putAll(newRequiredColumns);
			exportValues.add(row);
		};
		importJdbcTemplate.query("select * from " + table, rch);


		SimpleJdbcInsert insert = new SimpleJdbcInsert(this.exportJdbcTemplate).withTableName(table).usingColumns(exportColumnNames.toArray(new String[0]));
		Map[] importedRowArray = exportValues.toArray(new Map[0]);
		insert.executeBatch(importedRowArray);
	}

	private List<String> resolveExportColumnNames(Map<String, Object> newRequiredColumns, Map<String, String> renamedColumns, List<String> importColumnNames) {
		final List<String> exportColumnNames = importColumnNames.stream()
				.map(column -> renamedColumns.getOrDefault(column, column))
				.collect(Collectors.toList());
		exportColumnNames.addAll(newRequiredColumns.keySet());
		return exportColumnNames;
	}

	private Map<String, Object> convertToExportRowMap(List<String> columnNames, Map<String, String> renamedColumns, ResultSet rs) {
		return columnNames.stream()
				.filter(column -> extractColumnValue(rs, column) != null)
				.collect(Collectors.toMap(
						column -> renamedColumns.getOrDefault(column, column),
						column -> extractColumnValue(rs, column)
				));
	}

	private Object extractColumnValue(ResultSet rs, String column) {
		try {
			Object value = rs.getObject(column);
			if (value instanceof Blob) {
				Blob blob = (Blob) value;
				int length = (int) blob.length();
				System.out.println("length = " + length);
				return blob.getBytes(1, length);
			} else {
				return value;
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private List<String> extractColumnNames(JdbcTemplate jdbcTemplate, String table) {
		TableMetaDataContext tableMetadataContext = new TableMetaDataContext();
		tableMetadataContext.setTableName(table);
		tableMetadataContext.processMetaData(jdbcTemplate.getDataSource(), Collections.<String>emptyList(), new String[0]);

		return tableMetadataContext.getTableColumns();
	}

	private Set<String> getColumnNames(ResultSet rs) throws SQLException {
		Set<String> set = new HashSet<>();
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			String columnName = rs.getMetaData().getColumnName(i + 1);
			set.add(columnName);
		}
		return set;
	}
}
