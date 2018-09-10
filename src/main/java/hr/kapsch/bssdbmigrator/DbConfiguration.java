package hr.kapsch.bssdbmigrator;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class DbConfiguration {

	@Bean
	@ConfigurationProperties("datasource.import")
	public DataSourceProperties importDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean
	@ConfigurationProperties("datasource.import")
	public DataSource importDataSource() {
		return importDataSourceProperties().initializeDataSourceBuilder().build();
	}

	@Bean
	@Primary
	@ConfigurationProperties("datasource.export")
	public DataSourceProperties exportDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean
	@Primary
	@ConfigurationProperties("datasource.export")
	public DataSource exportDataSource() {
		return exportDataSourceProperties().initializeDataSourceBuilder().build();
	}

	@Bean(name = "exportJdbcTemplate")
	@Primary
	public JdbcTemplate exportJdbcTemplate() {
		return new JdbcTemplate(exportDataSource());
	}

	@Bean(name = "importJdbcTemplate")
	public JdbcTemplate importJdbcTemplate() {
		return new JdbcTemplate(importDataSource());
	}
}
