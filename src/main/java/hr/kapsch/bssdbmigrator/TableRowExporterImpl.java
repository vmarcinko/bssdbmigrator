package hr.kapsch.bssdbmigrator;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TableRowExporterImpl implements TableRowExporter {

	private final JdbcTemplate exportJdbcTemplate;

	public TableRowExporterImpl(JdbcTemplate exportJdbcTemplate) {
		this.exportJdbcTemplate = exportJdbcTemplate;
	}

	@Transactional
	public void exportToTable(String exportTable, List<String> exportColumnNames, List<Map<String, Object>> exportValues) {
		final SimpleJdbcInsert insert = new SimpleJdbcInsert(this.exportJdbcTemplate).withTableName(exportTable).usingColumns(exportColumnNames.toArray(new String[0]));
		Map[] importedRowArray = exportValues.toArray(new Map[0]);
		insert.executeBatch(importedRowArray);
	}
}
