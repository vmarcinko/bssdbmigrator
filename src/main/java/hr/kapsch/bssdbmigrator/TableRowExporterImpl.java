package hr.kapsch.bssdbmigrator;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TableRowExporterImpl implements TableRowExporter {
	private final Logger logger = LoggerFactory.getLogger(TableRowExporterImpl.class);

	private final JdbcTemplate exportJdbcTemplate;

	private final Random random = new Random(System.currentTimeMillis());

	public TableRowExporterImpl(
			@Qualifier("exportJdbcTemplate") JdbcTemplate exportJdbcTemplate) {
		this.exportJdbcTemplate = exportJdbcTemplate;
	}

	@Transactional
	public void exportToTable(String exportTable, List<String> exportColumnNames, List<Map<String, Object>> exportValues) {
		logger.info("Exporting {} rows to table {}", exportValues.size(), exportTable);

		final SimpleJdbcInsert insert = new SimpleJdbcInsert(this.exportJdbcTemplate).withTableName(exportTable).usingColumns(exportColumnNames.toArray(new String[0]));
		Map[] importedRowArray = exportValues.toArray(new Map[0]);
		insert.executeBatch(importedRowArray);

//		int i = random.nextInt(5);
//		if (i == 0) {
//			throw new RuntimeException("Impossible error");
//		}
	}
}
