package Common;

import StorageManager.StorageManager;
import Catalog.Catalog;

import java.sql.SQLSyntaxErrorException;

public interface Command {
    public boolean run(String[] command) throws SQLSyntaxErrorException;
}
