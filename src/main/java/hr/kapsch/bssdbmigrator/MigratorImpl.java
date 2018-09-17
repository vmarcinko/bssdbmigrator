package hr.kapsch.bssdbmigrator;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.metadata.TableMetaDataContext;
import org.springframework.stereotype.Service;

@Service
public class MigratorImpl implements Migrator {

	private final Logger logger = LoggerFactory.getLogger(MigratorImpl.class);

	private final JdbcTemplate importJdbcTemplate;
	private final JdbcTemplate exportJdbcTemplate;
	private final TableRowExporter tableRowExporter;
	private final int txBatchSize;

	public MigratorImpl(
			@Qualifier("importJdbcTemplate") JdbcTemplate importJdbcTemplate,
			@Qualifier("exportJdbcTemplate") JdbcTemplate exportJdbcTemplate,
			@Value("${txBatchSize}") int txBatchSize,
			TableRowExporter tableRowExporter) {
		this.tableRowExporter = tableRowExporter;
		this.importJdbcTemplate = importJdbcTemplate;
		this.exportJdbcTemplate = exportJdbcTemplate;
		this.txBatchSize = txBatchSize;
	}

	@Override
	public void migrate() {
		migrateTable("partners", Collections.singletonMap("cdr_traffic_enabled", true), Collections.emptyMap());
		migrateSequence("seq_partner");

		migrateTable("partner_tags");

		migrateTable("suspension_periods");
		migrateSequence("seq_suspension_period");

		migrateTable("users");
		migrateSequence("seq_user");

		migrateTable("service_elements");
		migrateSequence("seq_service_element");

		migrateTable("bt_services");
		migrateTable("sc_services");

		migrateTable("global_settings", Collections.emptyMap(), Collections.singletonMap("MONETARY_AMOUNT", "max_bt_unit_price"));

		migrateTable("traffic_reports");
		migrateTable("traff_sms_traffic_counts");
		migrateTable("traff_mms_traffic_counts");
		migrateTable("traff_bt_traffic_counts");
		migrateSequence("seq_traffic_report");

		migrateTable("financial_reports");
		migrateSequence("seq_financial_report");

		migrateTable("requests");
		migrateTable("request_bt_services");
		migrateTable("request_sc_services");
		migrateSequence("seq_request");

		migrateTable("events");
		migrateSequence("seq_event");

		migrateTable("documents");
		migrateSequence("seq_document");

		migrateTable("price_lists");

		migrateTable("sms_mo_message_fees");
		migrateTable("sms_throughput_usage_fees");
		migrateTable("sms_tmo_mt_message_fees");
		migrateTable("mms_mo_message_fees");
		migrateTable("mms_throughput_usage_fees");
		migrateTable("mms_tmo_mt_message_fees");

		migrateTable("profit_range_bonus_ptgs", Collections.emptyMap(), Collections.singletonMap("MONETARY_AMOUNT", "range_start"));
		migrateSequence("seq_price_list");

		Function<Number, Boolean> userTypeConverter = userTypeValue -> userTypeValue.intValue() == 0 ? true : false;
		migrateTable("subscribers", "subscriber_billings",
				Collections.emptyMap(), Collections.singletonMap("user_type", "prepaid"), Collections.singletonMap("user_type", userTypeConverter));

		Map<String, String> renamedColumns = new HashMap<>();
		renamedColumns.put("roaming_status", "in_roaming");
		renamedColumns.put("period_start", "interval_start");
		renamedColumns.put("period_end", "interval_end");

		migrateTable("roaming_history", "subscriber_roaming_intervals", Collections.emptyMap(), renamedColumns, Collections.emptyMap());

		migrateSequence("seq_roaming_history", "seq_subscriber_roaming_interval");
	}

	private void migrateSequence(String seqName) {
		migrateSequence(seqName, seqName);
	}

	private void migrateSequence(String importSeqName, String exportSeqName) {
		importSeqName = importSeqName.toLowerCase();
		exportSeqName = exportSeqName.toLowerCase();

		logger.info("Migrating sequence from '{}' to '{}'", importSeqName, exportSeqName);

		Number currentValue = this.importJdbcTemplate.queryForObject("select " + importSeqName + ".nextval from dual", Number.class);
		String exportSql = "select setval('" + exportSeqName + "', " + currentValue.intValue() + ", true)";
		this.exportJdbcTemplate.execute(exportSql);
	}


	private void migrateTable(String table) {
		migrateTable(table, Collections.emptyMap(), Collections.emptyMap());
	}

	private void migrateTable(String importTable, Map<String, Object> newRequiredColumns, Map<String, String> renamedColumns) {
		migrateTable(importTable, importTable, newRequiredColumns, renamedColumns, Collections.emptyMap());
	}

	private void migrateTable(String importTable, String exportTableOriginal, Map<String, Object> newRequiredColumnsOriginal, Map<String, String> renamedColumnsOriginal, Map<String, Function> valueConvertersOriginal) {
		importTable = importTable.toLowerCase();
		final String exportTable = exportTableOriginal.toLowerCase();

		Map<String, Object> newRequiredColumns = newRequiredColumnsOriginal.entrySet().stream()
				.collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));
		Map<String, String> renamedColumns = renamedColumnsOriginal.entrySet().stream()
				.collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), entry -> entry.getValue().toLowerCase()));
		Map<String, Function> valueConverters = valueConvertersOriginal.entrySet().stream()
				.collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));

		logger.info("Migrating from table '{}' to table '{}'", importTable, exportTable);

		final List<String> importColumnNames = extractColumnNames(this.importJdbcTemplate, importTable);

		final List<String> exportColumnNames = resolveExportColumnNames(newRequiredColumns, renamedColumns, importColumnNames);

		List<Map<String, Object>> exportValues = new ArrayList<>();
		RowCallbackHandler rch = rs -> {
			Map<String, Object> row = convertToExportRowMap(importColumnNames, renamedColumns, valueConverters, rs);
			row.putAll(newRequiredColumns);
			exportValues.add(row);

			if (exportValues.size() > txBatchSize) {
				tableRowExporter.exportToTable(exportTable, exportColumnNames, exportValues);
				exportValues.clear();
			}
		};
		importJdbcTemplate.query("select * from " + importTable, rch);

		if (!exportValues.isEmpty()) {
			tableRowExporter.exportToTable(exportTable, exportColumnNames, exportValues);
		}
	}

	private List<String> resolveExportColumnNames(Map<String, Object> newRequiredColumns, Map<String, String> renamedColumns, List<String> importColumnNames) {
		final List<String> exportColumnNames = importColumnNames.stream()
				.map(column -> renamedColumns.getOrDefault(column, column))
				.collect(Collectors.toList());
		exportColumnNames.addAll(newRequiredColumns.keySet());
		return exportColumnNames;
	}

	private Map<String, Object> convertToExportRowMap(List<String> columnNames, Map<String, String> renamedColumns, Map<String, Function> valueConverters, ResultSet rs) {
		return columnNames.stream()
				.filter(column -> extractColumnValue(rs, column) != null)
				.collect(Collectors.toMap(
						column -> renamedColumns.getOrDefault(column, column),
						column -> {
							final Object originalValue = extractColumnValue(rs, column);
							return valueConverters.containsKey(column) ? valueConverters.get(column).apply(originalValue) : originalValue;
						}
				));
	}

	private Object extractColumnValue(ResultSet rs, String column) {
		try {
			Object value = rs.getObject(column);
			if (value instanceof Blob) {
				Blob blob = (Blob) value;
				int length = (int) blob.length();
				return blob.getBytes(1, length);
			}
			else {
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

		return tableMetadataContext.getTableColumns().stream().map(String::toLowerCase).collect(Collectors.toList());
	}
}
