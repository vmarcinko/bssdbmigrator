package hr.kapsch.bssdbmigrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class BssDbMigrator {
	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(BssDbMigrator.class, args);
		Migrator migrator = context.getBean(Migrator.class);
		migrator.migrate();

		Thread.sleep(5000L);
	}
}
