package hr.kapsch.bssdbmigrator;

import java.util.List;
import java.util.Map;

public interface TableRowExporter {
	void exportToTable(String exportTable, List<String> exportColumnNames, List<Map<String, Object>> exportValues);
}
