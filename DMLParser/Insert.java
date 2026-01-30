package DMLParser;

import Catalog.Catalog;
import Common.Command;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;

public class Insert implements Command {

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        return false;
    }
}
