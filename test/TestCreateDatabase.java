import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.skellix.database.Database;
import com.skellix.database.Table;
import com.skellix.database.TableColumn;

class TestCreateDatabase {

	@Test
	void test() {

		String tabledef = "user {username 32 , password 32}";
		Path databasePath = Paths.get("testData", "testDatabase");
		
		if (Files.exists(databasePath)) {
			
			try {
				Files.walk(databasePath).forEach(path -> {
					
					System.out.println(path.toAbsolutePath().toString());
					
					if (!Files.isDirectory(path)) {
						
						try {
							Files.delete(path);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		HashMap<String, Table> database = Database.defineDatabase(databasePath.toFile(), tabledef);
		{
			Table userTable = database.get("user");
			{
				TableColumn[] row = userTable.getRow(0);
				
				TableColumn username = row[userTable.getColumnIndex("username")];
				username.setString("test");
				TableColumn password = row[userTable.getColumnIndex("password")];
				password.setString("testpass");
			}
		}
		{
			Table userTable = database.get("user");
			{
				TableColumn[] newRow = userTable.getRow(0);
				
				userTable.getRowFormat().entrySet().stream().forEach(entry -> {
					
					String key = entry.getKey();
					String value = newRow[userTable.getColumnIndex(key)].getString();
					System.out.println(key + ": " + value);
				});
			}
		}
	}

}
