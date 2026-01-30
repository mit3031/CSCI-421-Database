package Common;

import StorageManager.StorageManager;
import Catalog.Catalog;

public interface Command {
    public boolean run(String[] command);
}
